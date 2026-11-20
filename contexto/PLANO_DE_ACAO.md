# Plano de Ação — Trabalho 2 de Distribuição e Concorrência (2026/2): Livraria "Sebo" na AWS

> **Para a IA executora:** este plano foi montado a partir do enunciado `Trab2_2026_2.pdf` e dos resumos de aula (`RESUMO_CONTEXTO_PROJETO.md`, `trab_2_dist_conc/01-resumo-containers-kafka-kubernetes.md`, `trab_2_dist_conc/resumo-rodada-01-threads-ferramentas-rpc.md`). Todas as decisões abaixo foram **confirmadas pelo usuário**. **Use apenas tecnologias vistas em aula** (listadas na seção 2). Se algo não estiver coberto aqui, **pergunte ao usuário, não invente** (regra do próprio professor: "Na dúvida, não invente, pergunte!").
>
> **Passo 0:** salve uma cópia deste plano em `/Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab2/PLANO_DE_ACAO.md`.

---

## 1. Contexto

O enunciado pede uma aplicação de livraria estilo sebo na AWS com:
- **≥ 3 funções Lambda** (dica: dividir responsabilidades entre elas);
- **≥ 1 tabela** em banco de dados;
- **envio de e-mail**;
- **interface com o usuário** (comprador e colaborador).

Funcionamento: o comprador informa o livro que quer. **Se houver estoque**, recebe um e-mail dizendo para ir à livraria efetivar a compra. **Se não houver**, ele é avisado por e-mail **quando o livro chegar**. Tudo fica guardado no banco (estoque, pedidos). Supõe-se que o cliente **já se inscreveu no tópico SNS pelo console da AWS**.

Entrega: código no laboratório da AWS Academy, mais um relatório (o que funcionou e o que não funcionou, instalação, operação, **diagrama de blocos**, **diagrama UML de sequência** e a conta AWS Academy usada).

---

## 2. Decisões confirmadas com o usuário

| Tema | Decisão |
|---|---|
| Linguagem | **Python 3.12** (Lambdas e clientes) |
| Nuvem | AWS Academy Learner Lab, região **`us-east-1`**, role **`LabRole`** em todas as Lambdas |
| Banco | **DynamoDB** (como no lab Inventory), com **DynamoDB Streams** disparando Lambdas |
| E-mail | **SNS**, **um tópico único** `sebo-avisos`, e cada inscrição de e-mail com uma **subscription filter policy** `{"email": ["<email>"]}`. A Lambda publica com `MessageAttributes={"email": ...}` para só o cliente certo receber. ⚠️ A filter policy **não aparece nos slides** (o usuário escolheu assim mesmo); registrar isso no relatório. |
| Exposição HTTP | **Lambda Function URL**, Auth **NONE**, CORS habilitado (padrão dos slides; API Gateway só foi citado) |
| Pedido de livro **disponível** | **Reserva**: baixa o estoque na hora e o pedido fica `RESERVADO` |
| Vários clientes esperando quando o livro chega | **FIFO**: atende os pedidos mais antigos até acabar a quantidade que chegou; os demais continuam `AGUARDANDO` |
| Clientes | Tabela **`sebo-clientes`** **preenchida antes** por um script de carga (seed). Pedido com e-mail que não está na tabela é **recusado** |
| UI do colaborador | **Mínima**: registrar a chegada de livros (e listar o estoque para consulta). Sem cadastro de venda nem outras telas; os cenários são cobertos por **scripts de teste** |
| Interfaces | **CLI Python** (`input()` + `requests`, como o frontend do somador) **e** uma **página HTML+JS simples** servida com `python3 -m http.server` (como no `start.sh` dos slides) |
| Testes | **Cenários Python** em `testes/` que chamam as Function URLs **reais** na AWS, mais um **`testes.sh`** que roda todos ou um pelo nome (padrão `start.sh`/`stop.sh` dos slides). **Sem Makefile.** O README também traz os `curl ... \| json_pp` equivalentes e os eventos de teste do console |
| Infraestrutura | **Criação manual pelo console**, com roteiro passo a passo no README (como nos slides). Sem scripts de CLI/IaC |
| Prefixo de recursos | **`sebo-`** |
| Relatório | **Markdown + Mermaid** (`flowchart` para o diagrama de blocos e `sequenceDiagram` para o UML de sequência) |

### Tecnologias permitidas (vistas em aula) e uso
- **Lambda** (Python, `boto3` já incluído no runtime, **sem Layers**), **Function URL**, variáveis de ambiente, eventos de teste no console, `print()` + **CloudWatch Logs**.
- **DynamoDB** (`put_item`, `get_item`, `update_item`, `scan`) + **Streams** (`NEW_AND_OLD_IMAGES`) como gatilho de Lambda (padrão `Inventory`/`Check-Stock`).
- **SNS** (tópico Standard, inscrição Email, `sns.publish`).
- Cliente: **Python + `requests`**; HTML+JS com `fetch` (o frontend web do chat era HTML+JS em um arquivo só).
- Concorrência: **`threading`** em Python (conceito de threads/condição de corrida das aulas) só no cenário de teste concorrente.
- **Não usar:** API Gateway, Step Functions, RDS, SQS, S3, containers, Kafka, K8s nem frameworks web (Flask/Django). Não são necessários e o usuário não pediu.

---

## 3. Arquitetura

```
[CLI/HTML Comprador] --POST--> (λ sebo-pedido) --R/W--> [sebo-clientes] [sebo-livros] [sebo-pedidos]
[CLI/HTML Colaborador] --POST--> (λ sebo-chegada) --update qtd--> [sebo-livros]
[CLI/HTML/testes] --GET--> (λ sebo-consulta) --read--> tabelas
[script seed/testes] --POST--> (λ sebo-carga) --reset/seed--> tabelas

[sebo-livros] --Stream (qtd aumentou)--> (λ sebo-processa-espera) --FIFO: reserva pedidos AGUARDANDO--> [sebo-pedidos]
[sebo-pedidos] --Stream (virou RESERVADO)--> (λ sebo-notifica) --publish c/ MessageAttribute email--> (SNS sebo-avisos) --filtro--> e-mail do cliente
```

Princípio dos slides: **cada Lambda faz uma coisa só**. O e-mail sai **só** pela `sebo-notifica`, disparada pela mudança no banco, como no `Check-Stock`. Os dois casos (disponível agora e chegou depois) usam o mesmo caminho de notificação.

### 3.1 Tabelas DynamoDB (capacidade **On-demand**)

| Tabela | Partition key | Atributos | Stream |
|---|---|---|---|
| `sebo-livros` | `livro_id` (S) | `titulo` (S), `autor` (S), `preco` (N), `quantidade` (N) | **Sim**, `NEW_AND_OLD_IMAGES` |
| `sebo-clientes` | `email` (S) | `nome` (S) | Não |
| `sebo-pedidos` | `pedido_id` (S, `uuid4`) | `email`, `livro_id`, `status` (`AGUARDANDO`\|`RESERVADO`), `criado_em` (ISO-8601 UTC), `reservado_em` (opcional) | **Sim**, `NEW_AND_OLD_IMAGES` |

Para a FIFO, faça `scan` com filtro `livro_id = X AND status = AGUARDANDO` e ordene por `criado_em` em Python. Não usar GSI (não foi visto; o volume é pequeno). Trate a paginação do `scan` (`LastEvaluatedKey`).

### 3.2 Funções Lambda (todas Python 3.12, x86_64, role `LabRole`)

Variáveis de ambiente comuns: `TABELA_LIVROS=sebo-livros`, `TABELA_CLIENTES=sebo-clientes`, `TABELA_PEDIDOS=sebo-pedidos`. Na `sebo-notifica`: `SNS_TOPIC_ARN`.

Todas as que têm Function URL seguem o **modelo do somador** (`RESUMO_CONTEXTO_PROJETO.md` §1): cabeçalhos CORS, tratamento de `OPTIONS`, `json.loads(event.get("body") or "{}")`, respostas `{"statusCode", "headers", "body": json.dumps(..., default=str)}`, 400 para entrada inválida, 404 para entidade inexistente e 500 para erro inesperado, além de `print()` para o CloudWatch. Na Function URL o método vem em `event["requestContext"]["http"]["method"]`; aceite também `event.get("httpMethod")` como no slide. Converta `Decimal` do DynamoDB para `int`/`float` antes do `json.dumps`.

| # | Nome | Gatilho | Responsabilidade |
|---|---|---|---|
| 1 | `sebo-pedido` | Function URL (POST) | Body `{"email", "livro_id"}`. Valida campos (400), confere se o cliente existe em `sebo-clientes` (404 "cliente não cadastrado") e se o livro existe (404). Tenta **reservar** com `update_item` em `sebo-livros`: `SET quantidade = quantidade - 1` com **`ConditionExpression quantidade > 0`** (operação atômica, que evita a condição de corrida do último exemplar; ligar isso no relatório à aula de threads/condição de corrida). Se der certo, grava o pedido `RESERVADO` com `reservado_em`. Se der `ConditionalCheckFailedException`, grava o pedido `AGUARDANDO`. Responde `{"pedido_id", "status", "mensagem"}`. **Não envia e-mail.** |
| 2 | `sebo-chegada` | Function URL (POST) | UI do colaborador. Body `{"livro_id", "quantidade"}` (inteiro > 0). Livro inexistente dá 404. Faz `update_item` com `SET quantidade = quantidade + :q`. Responde com a nova quantidade. **Só isso.** A alocação fica com a próxima Lambda. |
| 3 | `sebo-processa-espera` | DynamoDB Stream de `sebo-livros` | Para cada record `MODIFY` (ou `INSERT`) em que `NewImage.quantidade > OldImage.quantidade` (ou > 0 no INSERT): busca os pedidos `AGUARDANDO` daquele livro em ordem FIFO. Para cada um, enquanto houver estoque: (a) baixa o estoque com o mesmo `update_item` condicional da `sebo-pedido`; se falhar, para; (b) `update_item` no pedido `SET status=RESERVADO, reservado_em=...` com `ConditionExpression status = AGUARDANDO`. Ignora records de diminuição de estoque, que são gerados pelas próprias reservas, para evitar loop. Em erro inesperado, `raise` (o Stream refaz a tentativa). |
| 4 | `sebo-notifica` | DynamoDB Stream de `sebo-pedidos` | Para cada record em que o pedido **passou a ser `RESERVADO`** (INSERT com status RESERVADO, ou MODIFY de AGUARDANDO para RESERVADO): busca o título em `sebo-livros` e o nome em `sebo-clientes`, e faz `sns.publish(TopicArn, Subject="Sebo: seu livro está reservado", Message="Olá <nome>, o livro '<título>' está reservado para você. Compareça à livraria para efetivar a compra. Pedido <id>.", MessageAttributes={"email": {"DataType": "String", "StringValue": <email>}})`. `print` do envio. |
| 5 | `sebo-consulta` | Function URL (GET) | Query string `?tipo=livros` (lista o estoque) ou `?tipo=pedidos&email=...` / `?tipo=pedido&pedido_id=...`. Usada pela UI (listar livros, ver meus pedidos) e pelos testes, para verificar o estado. |
| 6 | `sebo-carga` | Function URL (POST) | Usada só pelo seed e pelos testes. `{"acao": "reset"}` apaga todos os pedidos e recarrega livros e clientes a partir do body `{"acao": "seed", "livros": [...], "clientes": [...]}` com `put_item`. ⚠️ Fica pública (auth NONE): documentar no relatório como limitação didática e sugerir desativar a URL após a apresentação. |

> O enunciado exige ≥ 3 Lambdas: aqui são 6, cada uma com uma responsabilidade.

### 3.3 SNS
- Tópico **Standard** `sebo-avisos`.
- Para cada e-mail de cliente: *Create subscription*, protocolo **Email**, endpoint = e-mail, e **Subscription filter policy** (escopo *Message attributes*) = `{"email": ["<o mesmo e-mail>"]}`. Confirmar pelo link recebido. O enunciado considera isso já feito pelo cliente; documentar o passo no README.

---

## 4. Estrutura de diretórios a criar

Raiz: `/Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab2/sebo/`

```
sebo/
├── lambdas/
│   ├── sebo_pedido/lambda_function.py
│   ├── sebo_chegada/lambda_function.py
│   ├── sebo_processa_espera/lambda_function.py
│   ├── sebo_notifica/lambda_function.py
│   ├── sebo_consulta/lambda_function.py
│   ├── sebo_carga/lambda_function.py
│   └── eventos_teste/            # JSONs de evento de teste para o console (um por Lambda, incluindo exemplos de record de Stream)
├── clientes/
│   ├── requirements.txt          # requests
│   ├── config.py                 # lê as URLs de variáveis de ambiente (os.environ), com erro claro se faltar
│   ├── comprador_cli.py          # menu: listar livros, pedir livro (email + livro_id), ver meus pedidos, sair
│   └── colaborador_cli.py        # menu: listar estoque, registrar chegada (livro_id + quantidade), sair
├── web/
│   ├── index.html                # duas seções (Comprador / Colaborador), HTML+JS em um arquivo, fetch nas Function URLs
│   └── config.js                 # const URLS = {...}  (preenchido após o deploy; modelo em config.exemplo.js)
├── dados/
│   └── seed.json                 # livros (com pelo menos um de quantidade 0) e clientes (e-mails REAIS inscritos no SNS, preenchidos pelo usuário)
├── testes/
│   ├── comum.py                  # helpers: reset+seed, chamadas, espera com polling (timeout ~30 s) pelo processamento assíncrono dos Streams
│   └── cenario_*.py              # um arquivo por cenário (seção 5)
├── .env.exemplo                  # URL_PEDIDO=, URL_CHEGADA=, URL_CONSULTA=, URL_CARGA=
├── seed.sh                       # source .env; python clientes/... envia dados/seed.json para sebo-carga
├── testes.sh                     # ./testes.sh (todos) | ./testes.sh <nome_cenario>; venv + pip install -r; source .env; imprime OK/FALHOU por cenário
├── web.sh                        # cd web && python3 -m http.server --bind 0.0.0.0 8000
├── README.md                     # instalação (roteiro de console), operação, curl, eventos de teste, troubleshooting, limpeza
└── RELATORIO.md                  # relatório de entrega (seção 7)
```

---

## 5. Cenários de teste (`testes/cenario_*.py`)

Cada cenário começa com **reset + seed**, executa as chamadas como a UI faria, **verifica o estado pelo `sebo-consulta`** (com polling para os passos assíncronos) e imprime **qual e-mail deve ter chegado e para quem**, para conferir na caixa de entrada durante a apresentação.

1. `compra_disponivel`: livro com qtd 2 → pedido `RESERVADO`, qtd 1 → e-mail para o cliente A.
2. `compra_indisponivel`: livro com qtd 0 → pedido `AGUARDANDO`, nenhum e-mail.
3. `compra_indisponivel_depois_chega`: o anterior + `sebo-chegada` +1 → pedido vira `RESERVADO`, qtd volta a 0 → e-mail para A.
4. `fila_fifo`: A pede, depois B pede (qtd 0); chega +1 → só A `RESERVADO` e B `AGUARDANDO`; chega +1 → B `RESERVADO`. Dois e-mails, em ordem.
5. `chegada_maior_que_fila`: 1 cliente esperando, chegam 3 → 1 reservado e qtd final 2.
6. `ultimo_exemplar_concorrente`: livro com qtd 1, **duas threads** (`threading`) pedem ao mesmo tempo → exatamente 1 `RESERVADO` e 1 `AGUARDANDO`, qtd 0 (demonstra a condição de corrida resolvida pela escrita condicional).
7. `cliente_nao_cadastrado`: e-mail fora do seed → 404, nenhum pedido criado.
8. `livro_inexistente`: → 404.
9. `entrada_invalida`: body sem campos ou quantidade ≤ 0 na chegada → 400.

`testes.sh` aceita o nome do cenário (ex.: `./testes.sh compra_indisponivel_depois_chega`) e, sem argumento, roda todos em sequência, com resumo final.

---

## 6. Ordem de execução para a IA executora

1. **Salvar o plano** em `trab2/PLANO_DE_ACAO.md` (Passo 0).
2. **Escrever as 6 Lambdas** seguindo a seção 3.2 e o modelo do somador. Código simples, comentado em português, só `json`, `os`, `uuid`, `datetime`, `boto3` e `botocore.exceptions`.
3. **Escrever os eventos de teste** do console em `lambdas/eventos_teste/` (Function URL: `{"body": "{...}", "requestContext": {"http": {"method": "POST"}}}`; Stream: records `MODIFY` com `OldImage`/`NewImage` no formato DynamoDB `{"N": "1"}`).
4. **Escrever os clientes CLI** (`comprador_cli.py`, `colaborador_cli.py`), no estilo do frontend do somador: `input()`, `requests.post/get`, checagem de `status_code`, tratamento de `ValueError` e de exceções.
5. **Escrever `web/index.html`** (HTML+JS em um só arquivo, `fetch` com `Content-Type: application/json`) e `web.sh`.
6. **Escrever `dados/seed.json`, `seed.sh`, `testes/` e `testes.sh`.** Os e-mails do seed ficam como placeholders `<EMAIL_CLIENTE_A>` e `<EMAIL_CLIENTE_B>`. **Pergunte ao usuário** quais e-mails reais usar (precisam ser os inscritos no SNS).
7. **Escrever o README.md** com o roteiro de console, nesta ordem:
   1. Iniciar o Learner Lab e abrir o console (us-east-1).
   2. DynamoDB: criar as 3 tabelas (seção 3.1) e habilitar os Streams em `sebo-livros` e `sebo-pedidos` (*Exports and streams → Turn on → New and old images*).
   3. SNS: criar o tópico `sebo-avisos`, as inscrições de e-mail **com filter policy** e confirmar os e-mails.
   4. Lambda: para cada uma, *Author from scratch*, Python 3.12, x86_64, *Use existing role* `LabRole`, colar o código, **Deploy**, configurar as variáveis de ambiente e testar com o evento de teste.
   5. Function URL (Auth NONE, CORS: origin `*`, methods `GET, POST`, headers `content-type`) em `sebo-pedido`, `sebo-chegada`, `sebo-consulta` e `sebo-carga`.
   6. Triggers: `sebo-processa-espera` ← DynamoDB `sebo-livros` (batch size 1–10, starting position *Latest*); `sebo-notifica` ← DynamoDB `sebo-pedidos`.
   7. Copiar as URLs para `.env` e `web/config.js`, rodar `./seed.sh` e `./testes.sh`.
   - Seções extras: como operar (CLI, web), `curl` de cada endpoint, troubleshooting (CloudWatch Logs; **CORS duplicado**: se o navegador acusar cabeçalho `Access-Control-Allow-Origin` duplicado, deixar o CORS só na configuração da Function URL e retirar o cabeçalho do código, ou vice-versa; Stream sem trigger; e-mail não chega porque a inscrição está *Pending confirmation* ou o filtro está errado) e **limpeza** (desativar as Function URLs e apagar recursos após a nota).
8. **Escrever o RELATORIO.md** (seção 7).
9. **Validação local mínima** (sem AWS): `python3 -m py_compile` em todos os `.py` e `bash -n` nos `.sh`. A validação real fica com o usuário no Learner Lab (seção 8).

---

## 7. Conteúdo do `RELATORIO.md`

1. Identificação: integrantes, disciplina, **conta AWS Academy onde está a implementação** (placeholder `<preencher>`; perguntar ao usuário).
2. Visão geral e decisões de projeto (tabela da seção 2, com justificativas ligadas às aulas: Lambda de responsabilidade única como no lab Inventory, Streams, Function URL, condição de corrida e escrita condicional).
3. **Diagrama de blocos** em Mermaid `flowchart LR` (seção 3).
4. **Diagramas UML de sequência** em Mermaid `sequenceDiagram`: (a) compra com livro disponível; (b) compra indisponível → chegada → notificação (com Stream → `sebo-processa-espera` → Stream → `sebo-notifica` → SNS → e-mail).
5. Modelo de dados (tabelas).
6. Descrição de cada Lambda.
7. Instalação (resumo, apontando para o README) e operação.
8. Testes realizados (os cenários da seção 5) com resultado.
9. **O que funcionou / o que não funcionou** (a IA deixa a tabela com os resultados a preencher depois que o usuário rodar na AWS; não inventar resultados).
10. Limitações: filter policy fora dos slides; Function URLs públicas (auth NONE); `scan` em vez de índice; credenciais e sessão do Learner Lab.

---

## 8. Verificação ponta a ponta (feita pelo usuário no Learner Lab)

1. Seguir o README até o fim do roteiro de console.
2. Testar cada Lambda no console com os eventos de `lambdas/eventos_teste/` e conferir os prints no CloudWatch.
3. `./seed.sh`, depois `./testes.sh`: todos os cenários devem dar **OK**.
4. Conferir na caixa de entrada os e-mails esperados (cenários 1, 3 e 4) e **ver que o cliente B não recebe o e-mail do cliente A** (prova da filter policy).
5. `python clientes/comprador_cli.py` e `python clientes/colaborador_cli.py`: repetir o fluxo "pede indisponível → colaborador registra chegada → e-mail".
6. `./web.sh` e abrir `http://localhost:8000`: repetir o fluxo pela página.
7. Preencher a seção "O que funcionou / o que não funcionou" do relatório.

---

## 9. Pendências (perguntar ao usuário ou ao professor; não inventar)
- E-mails reais dos clientes de teste (seed e inscrições SNS). (thiagodc987@gmail.com. guigagratz@hotmail.com, gratzguilherme@gmail.com, ggratzbiladeiro@gmail.com)
- Nomes dos integrantes e qual conta AWS Academy informar no EaD. (Thiago Coqueiro, Guilherme Gratz, Felipe Peixoto, conta da aws no email: thiagodamicoc@gmail.com )
