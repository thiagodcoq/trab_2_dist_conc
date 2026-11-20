# Contexto do curso: Distribuição e Concorrência (PUC-Rio, Prof. Alexandre Meslin)

> Resumo dos slides/arquivos do professor, escrito para servir de contexto a outra IA que vai ajudar a fazer o trabalho.
> Fontes: `09 P2P.pdf`, `10 AWS.pdf`, `Microsserviços e Containers na AWS v3/v4.pdf`, `SQS.pdf`, `Secrets Manager.pdf`, `Calculadora.pdf`, `testaRDS-v1.py`, `mtcars.sqlite3`.
>
> **Aviso à IA que ler isto:** os slides são roteiros de laboratório (passo a passo no console AWS), não um enunciado único de "projeto". Os trechos que parecem pedir entregas/exercícios estão marcados com **[EXERCÍCIO]**. Em caso de dúvida sobre qual é o trabalho final, perguntar ao usuário. Os slides P2P (Java/Sinalgo) são um tema separado do AWS.

---

## 0. Visão geral: o que o professor quer que se use

| Tema | Tecnologias | Onde |
|---|---|---|
| Cloud | **AWS (conta AWS Academy / Learner Lab)**, região `us-east-1` | todos |
| Computação serverless | **AWS Lambda** (Python, `boto3`), **Function URL** (auth NONE, CORS) | Calculadora, SQS, AWS |
| Computação VM | **EC2** (Ubuntu, t2.micro) + Django | Microsserviços v1/v2 |
| Containers | **Docker**, **ECR**, **ECS** (EC2 launch type), **Application Load Balancer** | Microsserviços v3 |
| Banco relacional | **RDS MySQL** (db.t3.micro, free tier); SQLite local → migrar para MySQL | Microsserviços |
| Banco NoSQL | **DynamoDB** (+ DynamoDB Streams) | AWS (estoque) |
| Armazenamento | **S3** (buckets, eventos `ObjectCreated`) | SQS, AWS |
| Mensageria | **SNS** (tópicos, assinaturas e-mail/SQS), **SQS** (fila Standard) + **DLQ** | SQS, AWS |
| Segredos | **AWS Secrets Manager** | Secrets Manager |
| Observabilidade | **CloudWatch Logs** (prints da Lambda) | Calculadora, AWS |
| Shell | **CloudShell** (build/push de imagem Docker) | Microsserviços v3 |
| Backend web | **Django + Django REST Framework** (app `MTCars`) | Microsserviços |
| Cliente | **Python** (`requests`) desktop/CLI ou frontend simples | Calculadora, SQS |
| P2P (tema à parte) | **Java + Sinalgo** (simulador de redes), **Maven** | P2P |

Restrições do ambiente AWS Academy que aparecem repetidamente:
- Não dá para criar roles IAM/security groups novos em vários lugares → **usar `LabRole`** (Lambda, ECS task role/execution role) e **`LabInstanceProfile`** (instâncias EC2 do ECS), e reutilizar security group existente quando necessário.
- Mensagens de erro vermelhas por falta de permissão (Secrets Manager etc.) são esperadas; "dar Refresh e continuar".
- **Custos:** controlar gastos no Academy; remover recursos (serviços ECS, etc.) após uso.
- Nomes de bucket S3 são globais: acrescentar matrícula/data (ex.: `inventory-<matrícula>`, `image-app-2025-11-23`).

---

## 1. Lambda: Somador → Calculadora → Calculadora com microsserviços (`Calculadora.pdf`)

### Somador (modelo fornecido)
Função Lambda com suporte a CORS que soma dois reais recebidos via HTTP (Function URL / API Gateway).

Pontos do modelo:
- Cabeçalhos CORS: `Content-Type: application/json`, `Access-Control-Allow-Origin: *`, `Access-Control-Allow-Methods: OPTIONS,POST,GET`, `Access-Control-Allow-Headers: Content-Type`.
- Trata preflight: se `event.get("httpMethod") == "OPTIONS"` → 200 com `{"message": "CORS preflight OK"}`.
- O corpo chega como **string JSON**: `body = json.loads(event.get("body", "{}"))`; lê `a` e `b` com `float(...)`; devolve `{"a":..., "b":..., "soma":...}` (200).
- Qualquer exceção → 400 com `{"error": str(e)}`.

Esqueleto:
```python
import json
def lambda_handler(event, context):
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type",
    }
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": json.dumps({"message": "CORS preflight OK"})}
    try:
        body = json.loads(event.get("body", "{}"))
        a = float(body.get("a")); b = float(body.get("b"))
        return {"statusCode": 200, "headers": headers, "body": json.dumps({"a": a, "b": b, "soma": a + b})}
    except Exception as e:
        return {"statusCode": 400, "headers": headers, "body": json.dumps({"error": str(e)})}
```

### Como depurar (o professor indica 3 testes)
1. **Evento de teste no console Lambda:** `{"body": "{\"a\": 12.5, \"b\": 7.3}"}`.
2. **curl** na Function URL:
   `curl -X POST <FUNCTION_URL> -H "Content-Type: application/json" -d '{"a": 10.5, "b": 4.3}' | json_pp`
3. **`print()` no código** e ler em **CloudWatch → Log groups → (sua função) → log stream mais recente**.

### Frontend do somador (referência)
Script Python com `requests.post(LAMBDA_URL, headers={"Content-Type":"application/json"}, data=json.dumps({"a": a, "b": b}))`, lê `a` e `b` com `input()`, checa `status_code == 200` e imprime `data.get("soma")`; trata `ValueError` e exceções gerais.

### **[EXERCÍCIO]** pedidos
1. **Somador** (Lambda + frontend).
2. **Calculadora de 4 operações.**
3. **Calculadora Microsserviços:** dividir a calculadora em **quatro microsserviços** (um por operação: soma, subtração, multiplicação, divisão: cada um sua Lambda com Function URL) e **criar um frontend** que chama os quatro. (Lembrar de tratar divisão por zero → 400.)

---

## 2. Microsserviços e Containers na AWS (`Microsserviços e Containers na AWS-v4.pdf`; v3 é versão anterior com diferenças pequenas, a v4 prevalece)

**Objetivo:** implementar um serviço web com banco de dados em 4 versões evolutivas:
1. **V1** – Django + SQLite centralizado numa **EC2**.
2. **V2** – Django (EC2) + **RDS MySQL**.
3. **V3** – Django em **container** (ECR + ECS) + MySQL (RDS) + **balanceamento de carga** (ALB).
4. **V4** – serviço usando **funções Lambda** (+ RDS).

A aplicação de exemplo é **MTCars** (dataset `mtcars`, 34 carros). Endpoints REST: `GET /lista/` (todos) e `GET /umcarro/<id>/` (um). Código fornecido em `MTCars.tar.xz` (Django: `models.py`, `serializers.py`, `views.py`, `urls.py`, `settings.py`, `Dockerfile`, `requirements.txt`, `db.sqlite3`).

### 2.1 Passos iniciais: criar a EC2
- Console → **EC2 → Instances → Launch instance**.
- Name: ex. `meslin-mtcars-ec2`. OS: **Ubuntu**, AMI gratuita, **64-bit (x86)**. Instance type: **t2.micro**.
- **Key pair:** RSA, formato `.pem` (guardar com segurança). Se perder: gerar novo par, `ssh-keygen -y -f chave.pem`, parar instância, em *Edit user data* colocar um `cloud-config` com `ssh-authorized-keys`, reiniciar, testar, depois limpar o user data.
- **Security group:** permitir SSH (Anywhere), HTTP e HTTPS da internet.
- Storage/Advanced: default.
- Conectar: `chmod 400 chave.pem` e `ssh -i chave.pem ubuntu@<IP-publico>`.

### 2.2 Preparar a instância
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y --no-install-recommends \
  python3-venv python3-dev python3-pip python-is-python3 \
  build-essential pkg-config default-libmysqlclient-dev \
  netfilter-persistent iptables-persistent
sudo apt autoremove -y

# redirecionar porta 80 -> 8000 (Django)
sudo iptables -A INPUT -p tcp --dport 80 -j ACCEPT
sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 8000
sudo iptables -t nat -L -n -v          # verificar
sudo netfilter-persistent save         # (opcional) persistir
```
Baixar o código e extrair: `curl --output MTCars.tar.xz "<link do Drive do professor>"` e `tar -xvf MTCars.tar.xz`.

### 2.3 V1 – Monolito centralizado (Django + SQLite na EC2)
```bash
cd MTCars/
python3 -m venv venv && source venv/bin/activate
pip3 install -r requirements.txt
# em MTCars/MTCars/settings.py:  ALLOWED_HOSTS = ['*']
python3 manage.py runserver 0.0.0.0:8000      # (nohup ... para manter após fechar o terminal)
curl http://<public-dns-ec2>/lista/ | json_pp
curl http://<public-dns-ec2>/umcarro/5/ | json_pp
```

### 2.4 V2 – Separar o banco: RDS MySQL
Criar banco: console **Aurora and RDS → Databases → Create database → Full configuration**:
- Engine **MySQL** (Community); template **Free tier**.
- DB instance identifier `mtcarsDB`; master user `mtcarsUsername`; credentials **Self managed**; senha (nos slides: `mtcars123`); auth: Password.
- Instance class **db.t3.micro**; Storage **gp3, 20 GB**, **desmarcar storage autoscaling**.
- Connectivity: **Don't connect to an EC2** (para ter acesso público); Default VPC; subnet group `default-vpc`; **Public access: Yes**; novo security group `mysql-db-mtcars-sg`; porta **3306**; sem RDS Proxy.
- Additional config: **Initial database name `MTCars`**; desmarcar enhanced monitoring, backups automáticos, encryption, log exports, deletion protection.
- Depois de criado: editar o **security group** do RDS → *Inbound rules* → adicionar regra **MYSQL/Aurora, origem 0.0.0.0/0** (para permitir EC2/Lambda/CloudShell).

Migrar SQLite → MySQL:
1. `requirements.txt` deve conter: `django`, `djangorestframework`, `mysqlclient`, `pymysql`, `mysql-connector-python`.
2. `settings.py`:
```python
DATABASES = {
  'default': {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': 'MTCars',
    'USER': 'mtcarsUsername',
    'PASSWORD': '<senha>',
    'HOST': '<endpoint-do-rds>.us-east-1.rds.amazonaws.com',
    'PORT': '3306',
    'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"},
  }
}
ALLOWED_HOSTS = ['*']
```
3. `python3 manage.py makemigrations MTCars && python3 manage.py migrate`
4. Rodar **`testaRDS-v1.py`** (fornecido): testa conexão (`SELECT VERSION()` via `pymysql`), imprime o `CREATE TABLE` do SQLite, e após `<ENTER>` copia as linhas da tabela `MTCars` do SQLite para o MySQL (com `mysql.connector`, tabela criada se não existir). Editar `host/user/password/database/db_path/table_name`. Saída esperada: `34 registros copiados para MySQL na tabela MTCars`.
   - Esquema da tabela: `ID INT PK, NAME VARCHAR(255), MPG FLOAT, CYL INT, DISP FLOAT, HP INT, WT FLOAT, QSEC FLOAT, VS INT, AM INT, GEAR INT` (todos NOT NULL). O arquivo `mtcars.sqlite3` na pasta tem a tabela `MTCars` com esses dados.
5. `python3 manage.py runserver 0.0.0.0:8000` e testar `/lista/` e `/umcarro/5/` local e remotamente.

### 2.4.1 V3 – Container (Docker + ECR + ECS + ALB)
Recursos a criar (nomes de exemplo do professor):

| Recurso | Nome |
|---|---|
| Repositório ECR | `meslin/mtcars-repo` |
| Cluster ECS | `meslin-mt-ecs-cluster` |
| Task definition | `meslin-mt-task` |
| Container | `meslin-mt-container` |
| Service | `meslin-mt-ecs-service` |
| Load balancer | `meslin-mt-load-balancer` |
| Target group | `meslin-mt-target` |

Passos:
1. **CloudShell**: baixar e extrair o código como acima.
2. Ajustar `requirements.txt` e `settings.py` (como na V2) e **adicionar duas views e rotas de health check**:
```python
from rest_framework.decorators import api_view
@api_view(['GET'])
def home(request):   return Response({'msg':'ok'}, status.HTTP_200_OK)
@api_view(['GET'])
def health(request): return Response({'msg':'health'}, status.HTTP_200_OK)
```
```python
urlpatterns = [
  path("admin/", admin.site.urls),
  path("", views.home, name='home'),
  path('health/', views.health, name='health'),
  path("lista/", views.CarsView.as_view(), name='lista-carros'),
  path('umcarro/', views.CarView.as_view(), name='um-carro'),
  path('umcarro/<id_arg>/', views.CarView.as_view(), name='consulta-carro'),
]
```
3. **Dockerfile:**
```dockerfile
FROM python:3.14-slim
RUN apt update && apt install -y --no-install-recommends \
    build-essential pkg-config default-libmysqlclient-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /MTCars
COPY . .
RUN pip3 install -r requirements.txt
EXPOSE 8000
CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]
```
4. Testar local: `docker build --no-cache -t mtcars-repo .` → `docker run -d --name mtcars -p 8000:8000 mtcars-repo` → `curl localhost:8000` (`{"msg":"ok"}`) e `/health/` (`{"msg":"health"}`).
5. **ECR:** criar repositório (AES-256, sem scan on push) → **View push commands** → colar os comandos no CloudShell (no diretório do Dockerfile) → copiar a **Image URI** (`<conta>.dkr.ecr.us-east-1.amazonaws.com/<repo>:latest`).
6. **ECS cluster:** *Fargate and Self-managed instances*; novo ASG; **Spot**; AMI **Amazon Linux 2023**; instância **t2.micro**; instance role **`LabInstanceProfile`**; capacidade desejada **min=max=2**; VPC Default; **usar security group existente**; monitoring off.
7. **Task definition:** infraestrutura **Amazon EC2 instances** (não Fargate); task size **0.5 vCPU / 1 GB**; task role e execution role = **`LabRole`**; container com a Image URI e **container port 8000**.
8. **Service:** *Launch type = EC2*; networking com grupo default; **Application Load Balancer** (`meslin-mt-load-balancer`), target group `meslin-mt-target`, **porta 80**.
9. Se falhar: ECS → cluster → service → aba **Events**. Problema comum: **security group do ALB sem regra de entrada HTTP (0.0.0.0/0)** → EC2 → Load Balancers → Security groups → *Edit inbound rules* → HTTP anywhere. Testar pelo DNS do load balancer.
10. Remover serviço (CloudShell):
```bash
aws ecs update-service --cluster meslin-mt-ecs-cluster --service meslin-mt-ecs-service --desired-count 0
aws ecs delete-service --cluster meslin-mt-ecs-cluster --service meslin-mt-ecs-service
aws ecs list-services --cluster meslin-mt-ecs-cluster
```
Comandos úteis: `aws ecs deregister-task-definition --task-definition <fam>:<rev>`; `sudo docker builder prune`.

### 2.5 V4 – Lambda + RDS (`listaCarros`)
- Lambda `listaCarros`: Author from scratch; runtime Python mais recente (ou o mesmo da sua máquina, ex. 3.12); **x86_64**; role existente **`LabRole`**; **Function URL, Auth NONE, Invoke mode BUFFERED, CORS habilitado**.
- **Variáveis de ambiente:** `RDS_ENDPOINT`, `RDS_USERNAME`, `RDS_PASSWORD`, `RDS_DATABASE` (= `MTCars`).
- Código: `pymysql.connect(host, user, password, database, cursorclass=pymysql.cursors.DictCursor)`, `SELECT * FROM MTCars`, retorna `{'statusCode': 200, 'body': rows}` (ou 500 em erro). Observação: para Function URL, o ideal é `json.dumps(rows, default=str)` no body, pois `body` deve ser string; o slide do professor devolve o dict direto.
- **Dependências via Lambda Layer:**
  ```bash
  # requirements.txt: pymysql
  pip install -r requirements.txt -t python/
  zip -r listaCarros-lambda.zip python/
  ```
  Console Lambda → Layers → *Create layer* (nome `listaCarros`, upload do zip, arquitetura x86_64, runtime compatível) → na função, *Add a layer → Custom layers*.
- Testar: criar test event `{}` (`testaListaCarros`), **Deploy**, **Test**; ou `curl <function-url>`.
- **Troubleshooting:** conferir config de **VPC** da Lambda e a **conectividade/security group do RDS** (acesso público, regra 3306).

---

## 3. SNS + SQS + Lambda: processamento assíncrono de imagens (`SQS.pdf`)

**Objetivo:** aplicação **distribuída e fracamente acoplada** para processamento assíncrono de imagens usando **Lambda, S3, SNS, SQS + DLQ e cliente desktop Python**.

### Fluxo
1. Usuário envia imagem via **cliente desktop** (Python).
2. **Lambda de upload** (`imageUploadService`, Function URL) grava no **S3** (bucket 1).
3. S3 emite evento `ObjectCreated` → **SNS** (`uploadnotification`).
4. SNS replica o evento para os interessados: aqui, uma **fila SQS** (`ImageApp`).
5. A fila aciona a **Lambda processadora** (`process-image-remove-bg`), que **remove o fundo** da imagem e grava o resultado no **bucket 2**.
6. O bucket 2 dispara evento para **outro SNS**, que envia **e-mail** ao usuário.
7. Se exceder o tempo ou falhar várias vezes, o SQS manda a mensagem para a **DLQ**.

### Infraestrutura (passo a passo)
- **S3:** bucket *General purpose* `image-app-<sufixo>`; desmarcar *Block all public access* (confirmar o aviso). Segundo bucket de saída (ex.: `image-app-down-<sufixo>`).
- **Lambda de upload** `imageUploadService`: runtime = versão Python do seu desktop; role **`LabRole`**; **Function URL, auth NONE**. Recebe JSON `{"filename":..., "filedata": <base64>}`; valida; `base64.b64decode`; chave `uploads/{timestamp UTC}-{filename}` (evita sobrescrever); `s3.put_object(Bucket, Key, Body, ContentType)` (jpg/jpeg → `image/jpeg`, png → `image/png`, resto → `application/octet-stream`); responde 200 com `file_url`, 400 para parâmetros ausentes/JSON inválido, 500 para erro; sempre com cabeçalhos CORS. Teste com curl (Linux): `-d '{"filename":"teste.png","filedata":"'"$(base64 -w 0 fork.png)"'"}'`.
- **SQS:** *Create queue*, tipo **Standard**, nome `ImageApp`.
- **SNS:** tópico **Standard** `uploadnotification`. Editar **Access policy** (JSON) restringindo a publicação ao seu S3 com `Condition`: `ArnLike aws:SourceArn = arn:aws:s3:::<bucket>` e `StringEquals aws:sourceaccount = <id da conta>`. Criar **Subscription**: protocolo **Amazon SQS**, endpoint = ARN da fila `ImageApp`.
- **S3 → SNS:** bucket → *Properties → Event notifications → Create event notification* (`SendtoSns`), tipo **All object create events**, destino **SNS topic** `uploadnotification`.
- **Lambda processadora** `process-image-remove-bg`:
  - Gatilho: **SQS** (`ImageApp`) via *+ Add trigger*.
  - Cada record SQS contém o evento S3 embrulhado pelo SNS: `json.loads(body)["Message"]` → `json.loads(...)` (com fallback para S3→SQS direto). Extrai `bucket` e `key` de `Records[0].s3`.
  - **`robust_get_object`:** retry com backoff exponencial (5 tentativas, 0.5 s·2^i) para `NoSuchKey`, porque o evento do S3 pode chegar antes do objeto estar disponível.
  - `remove()` usa a biblioteca **`rembg`** (no modelo está comentado; precisa de **Layer** com `rembg` no `requirements.txt`).
  - Grava `key_semfundo.png` no bucket de saída (`ContentType image/png`). Em erro: `raise` (para a mensagem voltar à fila / ir para a DLQ).
- **Testar:** usar o cliente desktop para subir uma imagem e verificar o arquivo sem fundo no segundo bucket.

### **[EXERCÍCIO]** "Trabalhos futuros" (pedidos)
1. **Notificação do usuário:** Lambda invocada quando o arquivo processado é gerado, que envia **e-mail** com a **URL de download**. Dica: `s3.generate_presigned_url("get_object", Params={"Bucket":..., "Key":...}, ExpiresIn=3600)`. Bibliografia: docs do boto3 S3 / `generate_presigned_url`.
2. **Implementar a DLQ** para tratamentos de imagem que falharam. Para testar, usar uma Lambda que sempre falha: `raise Exception("Erro proposital para testar DLQ")`.
3. **Cliente desktop Python** que sobe imagens (o slide menciona o "programa frontend desktop", mas o código não está no PDF; precisa ser implementado: ler arquivo, base64, POST na Function URL do upload).

---

## 4. AWS Secrets Manager (`Secrets Manager.pdf`)

Serviço para guardar/gerenciar credenciais (usuário/senha de BD, chaves de API) **fora do código-fonte e arquivos de configuração**.

Criar segredo (console → Secrets Manager → *Store a new secret*):
- Tipo: **Credentials for Amazon RDS database**; informar usuário/senha do MySQL MTCars; escolher a chave de criptografia e o banco (o RDS).
- Nome: `mtcars/mysql/credenciais`; descrição livre. Rotação: não alterar. *Store*.
- Esperados erros de permissão no Academy; **Refresh** e seguir. Anotar o **Secret ARN**.

Recuperar na Lambda:
```python
import json, os, boto3
DB_HOST = os.environ["DB_HOST"]; DB_NAME = os.environ["DB_NAME"]; SECRET_ARN = os.environ["SECRET_ARN"]
secrets_client = boto3.client("secretsmanager")

def obter_credenciais():
    r = secrets_client.get_secret_value(SecretId=SECRET_ARN)
    return json.loads(r["SecretString"])     # dict com username/password
```
Variáveis de ambiente da Lambda: `DB_HOST` (endpoint do RDS), `DB_NAME` (`MTCars`), `SECRET_ARN`. Resultado vê-se na aba OUTPUT/CloudWatch.

> Boa prática (não está no slide): **não** imprimir senha em logs nem devolvê-la no `body`, como o exemplo do professor faz para fins didáticos; use o segredo para abrir a conexão `pymysql` e só.
> Este é o caminho "certo" para substituir `RDS_PASSWORD` em texto puro nas variáveis de ambiente da V4 da seção 2.

---

## 5. Sistema de controle de estoque (Lab AWS, `10-...AWS.pdf`)

Slides de aula sobre AWS, comparando provedores (AWS, Azure, GCP, Rackspace, IBM Cloud, Cloud Foundry) e um **laboratório guiado** (pré-requisito: Lab 14 de *Cloud Architecture* da AWS Academy, que fornece o código e os CSVs).

### Arquitetura
1. Upload de **CSV** de estoque (uma ou mais lojas) para um bucket **S3** (`inventory-<matrícula>`).
2. **Trigger do S3** chama a Lambda `Load-Inventory`, que lê o CSV e grava no **DynamoDB** (tabela `Inventory`).
3. A **inserção na tabela dispara (DynamoDB Stream)** a Lambda `Check-Stock`, que detecta itens com `Count == 0`.
4. `Check-Stock` publica no **SNS** (tópico `NoStock`) → **e-mail** de alerta.

Princípio de design explicitado: **cada Lambda faz uma coisa só** (não colocar a verificação de estoque dentro de `Load-Inventory`); mais lógica = mais Lambdas independentes.
**Quando NÃO usar Lambda:** função que demora > **15 min** ou usa > **10 GB** de memória.

### Passo a passo resumido
- **Lambda `Load-Inventory`:** Python ≥ 3.10, role **`LabRole`** (acesso a S3 e DynamoDB). Código: pega `bucket`/`key` de `event['Records'][0]['s3']`, `urllib.parse.unquote_plus(key)`, baixa para `/tmp/inventory.txt` (`s3.meta.client.download_file`), lê com `csv.DictReader` e faz `inventoryTable.put_item(Item={'Store': row['store'], 'Item': row['item'], 'Count': int(row['count'])})`. Salvar + **Deploy**.
- **S3 + evento:** criar bucket (nome único); *Properties → Event notifications → Create* (`Load-Inventory`), **All object create events**, destino **Lambda function `Load-Inventory`**.
- **DynamoDB:** tabela `Inventory`, **partition key `Store` (String)**, **sort key `Item` (String)**. Habilitar **stream** (necessário para o trigger da Check-Stock; formato `NEW_AND_OLD_IMAGES`).
- **CSV de exemplo:** cabeçalho `store,item,count` (ex.: `Berlin,Echo Dot,12`...; cada arquivo tem um item com estoque 0).
- **SNS:** tópico Standard `NoStock`; *Subscription* protocolo **Email** → confirmar no e-mail recebido.
- **Lambda `Check-Stock`:** role `LabRole`; para cada record com `NewImage`, se `int(Count['N']) == 0` monta `"<store> is out of stock of <item>"`, descobre o ARN do tópico via `sns.list_topics()` (termina em `:NoStock`) e faz `sns.publish(TopicArn, Message, Subject='Inventory Alert!')`. Trigger: **DynamoDB → tabela `Inventory`**.
- **Testar:** subir CSVs no S3; ver itens em *DynamoDB → Tables → Inventory → Explore table items*; ver logs em *Lambda → Monitor → View CloudWatch logs*. Pergunta do professor: "o que acontece se subir vários arquivos ao mesmo tempo?" (concorrência: Lambdas executam em paralelo; `put_item` por chave Store+Item é idempotente/sobrescreve).
- Detalhes úteis: o handler recebe `event` e `context`; function URL tem auth `AWS_IAM` ou `NONE` + CORS; tags servem para organização/custo/controle de acesso; Lambdas rodam por padrão numa VPC gerenciada, mas podem acessar VPC própria.

### Catálogo de serviços mencionados (conceitos que o professor quer que se saiba)
- **DynamoDB:** NoSQL gerenciado, chave-valor/documento, latência de ms em qualquer escala.
- **RDS:** relacional gerenciado (MySQL, PostgreSQL, MariaDB, Oracle, SQL Server).
- **Neptune:** banco de **grafos**. **Redshift:** data warehouse (petabytes). **DocumentDB:** compatível com MongoDB.
- **S3:** armazenamento de objetos. **EFS:** sistema de arquivos NFS elástico.
- **API Gateway:** criar/publicar/monitorar APIs. **Step Functions:** orquestração serverless de workflows.
- **SNS** (pub/sub, notificações), **SQS** (filas), **Lambda** (executar código sem gerenciar servidores).

### **[EXERCÍCIO]** pedido no final
> Implementar uma **interface com o usuário** para entrar com os dados do estoque: o usuário informa a **loja** e **vários produtos com suas quantidades**; a entrada termina quando o usuário digita um **produto com nome vazio** (como em Prog1). (Ou seja, um cliente que monta o CSV/dados e os envia ao sistema S3/Lambda/DynamoDB.)

---

## 6. P2P: Peer-to-Peer e simulação com Sinalgo (`09-...P2P.pdf`)

### Teoria (resumo)
- **P2P:** cada participante (peer) é cliente e servidor ao mesmo tempo; compartilham recursos diretamente, sem intermediário.
- Tipos: **descentralizada** (peers com igual responsabilidade), **centralizada** (depende de servidor para iniciar/terminar operações), **híbrida** (há elementos centralizadores).
- Exemplos históricos: Napster (servidor central), KaZaA (semi-centralizado), Gnutella, (Bit)Torrent. Aplicações: torrent, VoIP, jogos online, redes de sensores, SETI@home/BOINC, **blockchain** (Bitcoin, Ethereum: descentralização, segurança, imutabilidade).
- Conceitos: **DHT** (tabela hash distribuída, ex. **Kademlia**), protocolo BitTorrent; técnicas de busca/roteamento: **DHT, inundação (flooding), roteamento baseado em interesses** (Rumor Routing em redes de sensores).

### Estudo de caso: gado com sensores (comunicação oportunística)
- Cada animal tem sensor de posição que **gera novos registros periodicamente** (com local e horário). Controladores com **rádio de curto alcance** (baixo consumo). O gado anda livremente.
- **Quando 2 animais se aproximam**, os controladores trocam **todos os NOVOS dados** acumulados; guardam só o que é novo.
- **Quando um animal chega perto de uma estação estacionária**, esvazia o buffer enviando **todos os dados**.
- Comparação **Peer2Peer vs Peer2Server**.

### Implementação no simulador **Sinalgo** (Java, Maven)
- Sinalgo: framework de simulação focado em **verificar algoritmos de rede** (visão de passagem de mensagens, abstrai camadas inferiores), mesmo para redes sem fio. Site: https://sinalgo.github.io/
- Projeto `sinalgo-maven` (Java 1.8, `maven-shade-plugin` para gerar JAR com `mainClass sinalgo.Run`, dependência `jdom2`). Projeto: `projects/p2pDistribuicaoConcorrencia`.
- Pacotes: `models` (conectividade/distribuição), `nodes.edges`, `nodes.messages`, `nodes.nodeImplementations`, `nodes.timers`; recursos: `resources/projects/.../Config.xml` (dimensões 100×50, mobilidade ligada, modo síncrono), `resources/sinalgo/appConfig.xml`.
- Classes principais:
  - **`MyNode`** (animal, móvel): `init()` cria seu `RecordCollectionManager` e uma **thread `RecordCreator`** que gera registros; `neighborhoodChange()` detecta **novos vizinhos** e envia `IdMessage` com os IDs das coleções que conhece; `handleMessages()` despacha por tipo de mensagem (mapa `Class → BiConsumer`).
  - **`MyStationaryNode`** (estação fixa, verde): mesmo protocolo de mensagens, coleta dados.
  - **`RecordCollectionManager`** (por nó de origem): `nodeId`, `nextRecord`, `lastSavedRecord` (inicia em −1), lista de `RecordEntry`.
  - **`RecordEntry`**: `recordNumber` + `payload`; **imutável** (sem setters).
- **Protocolo de mensagens:**
  - `IdMessage(nodeId)`: apresenta-se/informa de quem tem dados.
  - `LastSavedRecordMessage(nodeId, lastSavedRecordNumber)`: informa o último registro salvo que conhece daquele nó.
  - `NextRecordNumberMessage(nodeId, nextRecordNumber)`: pede mais registros a partir do número seguinte ao último recebido.
  - `RecordListMessage(nodeId, recordList)`: envia a lista de registros pedidos.
  - Ideia: troca incremental (anti-entropia), só envia o que o vizinho ainda não tem, identificando por (nó de origem, número do registro).
- Este bloco é um **tema separado** do AWS; usar apenas se o trabalho envolver P2P/simulação.

---

## 7. Cheat sheet de padrões e boas práticas (o que o professor demonstra)

1. **Serverless orientado a eventos:** S3 → (Lambda | SNS → SQS → Lambda); DynamoDB Stream → Lambda; Lambda → SNS → e-mail.
2. **Desacoplamento:** SNS faz fan-out; SQS faz buffer/retry; **DLQ** captura falhas; uma função = uma responsabilidade.
3. **Idempotência/retry:** o S3 pode avisar antes do objeto estar legível → retry com backoff exponencial; evitar sobrescrever com timestamp na chave.
4. **Segredos fora do código:** Secrets Manager (ou ao menos variáveis de ambiente); nunca hardcodar senha de produção.
5. **CORS** e preflight (`OPTIONS`) em toda Lambda exposta via Function URL; corpo vem como string JSON.
6. **Dependências na Lambda:** Layers (`pip install -r requirements.txt -t python/` + zip); runtime/arquitetura da layer devem bater com a função.
7. **Deploy EC2 simples:** Django em 8000, `iptables` redirecionando 80 → 8000, `ALLOWED_HOSTS=['*']`.
8. **Container:** Dockerfile → ECR → ECS (task definition → service) atrás de **ALB**, com rotas `/` e `/health/` para health check; conferir security group do ALB.
9. **Logs/depuração:** `print` + CloudWatch; eventos de teste no console; `curl | json_pp`.
10. **Custos e limpeza:** apagar serviços/instâncias ao terminar; usar free tier (t2.micro, db.t3.micro, gp3 20 GB).

---

## 8. Dados/credenciais de exemplo que aparecem nos materiais

Valores **didáticos do professor** (conta AWS Academy temporária); não reutilizar em produção:
- RDS MySQL: usuário `mtcarsUsername` (em `testaRDS-v1.py`: `adminUsername`), senha `mtcars123`, schema `MTCars` (em `testaRDS-v1.py`: `demoDB`), porta 3306.
- Segredo: `mtcars/mysql/credenciais`.
- Nomes de recursos usam o prefixo `meslin-...`: substituir pelo seu próprio.
- Endpoints, ARNs, IDs de conta e URLs de Function URL nos slides são **exemplos**; cada aluno deve usar os seus.

## 9. Itens ambíguos / a confirmar com o usuário
- Qual combinação de laboratórios compõe o **trabalho final** (ex.: calculadora com microsserviços? app de imagens com DLQ + e-mail? estoque com UI? MTCars em V1–V4?). Os slides trazem vários exercícios.
- Os slides não trazem o código do **cliente desktop** de imagens nem do **frontend da calculadora** (apenas um exemplo de CLI para o somador).
- No slide de Secrets Manager e na V4 há credenciais em texto puro; avaliar melhorar com Secrets Manager.
- O código do `Check-Stock` e do `Load-Inventory` vem do **Lab 14 de Cloud Architecture** (AWS Academy), fora dos PDFs.
