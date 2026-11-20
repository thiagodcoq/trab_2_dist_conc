# Resumo das aulas — Distribuição e Concorrência (Prof. Alexandre Meslin, PUC-Rio)

> **Rodada 1** — 3 PDFs resumidos:
> 1. `03-Distribuicao-Concorrencia_-_Containers.pdf` (Docker, Docker Compose, Kafka, WebSocket, Java/Maven)
> 2. `03_5-Distribuicao-Concorrencia_Kubernets-v2.pdf` (Kubernetes — conceitos)
> 3. `03_5-Distribuicao-Concorrencia_Kubernets-v3.pdf` (Kubernetes — conceitos **+ manifestos YAML**)
>
> **Relação entre v2 e v3:** a v3 é a v2 completa (slides 1–27 idênticos) acrescida de uma segunda parte sobre arquivos YAML/manifestos (slides 28–62). Tudo o que está na v2 está na v3. Na v2 o slide 23 já mostra um YAML conceitual e o slide 24 mostra o `kubectl`; na v3 isso é aprofundado.

---

## Visão geral: o que o professor quer que a gente domine

| Tema | Tecnologia | Para quê |
|---|---|---|
| Arquitetura de microsserviços | Serviços separados (produtor / consumidor) | Demonstrar desacoplamento |
| Comunicação entre serviços | **Apache Kafka** (event streaming) | Mensageria eficiente e durável |
| Tempo real cliente↔servidor | **WebSocket** (Java, Tyrus) | Chat bidirecional |
| Empacotamento | **Docker** (`Dockerfile`) | Imagens portáveis |
| Orquestração local | **Docker Compose** (`docker-compose.yml`) | Subir vários containers juntos |
| Build Java | **Maven** (`pom.xml`) | Gerar JARs com dependências |
| Orquestração em cluster | **Kubernetes (K8s)** + `kubectl` + YAML | Escalar, auto-recuperar, atualizar sem parar |

**Objetivos declarados da aula de containers (slide 3):**
- Demonstrar a arquitetura de microsserviços
- Utilizar Kafka para comunicação eficiente entre serviços
- Empacotar serviços usando Docker
- Gerenciar serviços usando Docker Compose

> ⚠️ **Nota:** estes 3 PDFs **não** mencionam Claude, AWS nem banco de dados como requisito. O único "banco" que aparece é o MySQL no exemplo de Secret do Kubernetes (`mysql.yaml`, laboratório *SmartFactory_Lab*). Se o enunciado do projeto exigir outras tecnologias, elas virão em outros slides.

---

# PARTE 1 — Containers, Kafka e o projeto "Chat App"

## 1.1 Contexto conceitual

### Event streaming (slide 5, fonte: kafka.apache.org/intro)
- É o "sistema nervoso central digital": capturar dados em tempo real de fontes (bancos, sensores, mobile, cloud, apps) como **streams de eventos**; armazená-los de forma **durável**; processá-los/reagir em tempo real **e** retrospectivamente; e **rotear** para diferentes destinos.
- Garante fluxo contínuo de dados: "a informação certa, no lugar certo, na hora certa".

### Apache Kafka (slide 10)
- Plataforma distribuída de armazenamento de eventos e processamento de streams. Open-source (Apache), escrita em Java e Scala.
- Objetivo: plataforma unificada, de **alto throughput e baixa latência** para feeds de dados em tempo real.
- **Kafka Connect**: integra com sistemas externos (import/export). **Kafka Streams**: biblioteca de processamento de streams.
- Protocolo binário sobre TCP, com abstração de "message set" (agrupa mensagens → menos round-trips, escritas sequenciais em disco).

### ZooKeeper (slide 11)
- Serviço centralizado para configuração, nomes, sincronização distribuída e serviços de grupo.
- **Importante (slides 7–9 e 15):** os slides mostram a arquitetura *com* ZooKeeper e depois *sem* ele (marcado com X). No slide 15 há a anotação: **"O Zookeeper não é mais necessário. Configurar o cluster para trabalhar no modo KRaft."** O exemplo final de Kafka já usa KRaft (`KAFKA_PROCESS_ROLES: broker,controller`), mas o `docker-compose.yml` ainda mantém um serviço `zookeeper` (legado do exemplo).

### Arquitetura do Chat (slide 6)
Quatro componentes:
1. **Producer Service** — envia mensagens para um tópico Kafka.
2. **Consumer Service** — lê mensagens de um tópico Kafka e as transmite aos clientes.
3. **WebSocket Server** — comunicação em tempo real cliente↔servidor.
4. **Frontend** — aplicação web simples de chat. (Nota no slide: o frontend *deixou de ser uma aplicação web* para se adaptar ao ambiente **GitHub Codespaces** → na prática, passaram a ser scripts Python no terminal. Ver 1.6.)

Fluxo: **Cliente → WebSocket (porta 8080) → ChatProducer → Kafka (tópico `chat-messages`) → ChatConsumer → WebSocket (porta 8081) → Clientes ouvintes**.

## 1.2 Estrutura de diretórios (slide 12)

```
chat-app/
├── consumer-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/br/com/meslin/
│       ├── ChatConsumer.java
│       └── WebSocketServer.java
├── producer-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/br/com/meslin/
│       ├── ChatProducer.java
│       └── WebSocketServer.java
└── docker-compose.yml
```
(Mais `frontend/` com os scripts Python/HTML e `start.sh`/`stop.sh` na raiz — ver 1.6 e 1.7.)

## 1.3 Docker Compose (slides 13–19)

`docker compose` **cria e levanta** as imagens: Zookeeper, Kafka, Consumidor e Produtor.

### Redes (slide 14)
```yaml
version: '3'
networks:
  ext-network:
    driver: bridge
  int-network:
    driver: bridge
    internal: true
```
- `int-network`: rede **interna** (sem acesso externo), driver `bridge`.
- `ext-network`: rede **externa**.

### Zookeeper (slide 15)
```yaml
services:
  zookeeper:
    image: zookeeper:3.6.3
    ports:
      - "2181:2181"          # host:container
    deploy:
      resources:
        limits:
          memory: 512M       # máximo
        reservations:
          memory: 256M       # mínimo
    networks:
      - int-network
```

### Kafka em modo KRaft (slides 16–17)
```yaml
  kafka:
    image: apache/kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data
    deploy:
      resources:
        limits:
          memory: 1G
        reservations:
          memory: 512M
    networks:
      - int-network
```
Variáveis explicadas no slide 17:
- `KAFKA_ADVERTISED_LISTENERS`: endereço que o Kafka **anuncia** para os outros componentes.
- `KAFKA_LISTENERS`: endereço onde o Kafka **escuta** (comunicação interna).
- `KAFKA_ZOOKEEPER_CONNECT`: endereço do Zookeeper (modo antigo, sem KRaft).
- Documentação: https://hub.docker.com/r/apache/kafka

### Producer e Consumer services (slides 18–19)
```yaml
  producer-service:
    build: ./producer-service        # constrói a partir do Dockerfile do diretório
    ports:
      - "8080:8080"
    depends_on:
      - kafka                         # só inicia depois do kafka
    deploy:
      resources:
        limits:   { memory: 512M }
        reservations: { memory: 256M }
    networks:
      - int-network
      - ext-network

  consumer-service:
    build: ./consumer-service
    ports:
      - "8081:8080"                   # externo 8081 → interno 8080
    depends_on:
      - kafka
    deploy:
      resources:
        limits:   { memory: 512M }
        reservations: { memory: 256M }
    networks:
      - int-network
      - ext-network
```
**Padrão:** serviços de aplicação ficam nas **duas** redes (falam com o Kafka pela interna e com o cliente pela externa); Kafka/Zookeeper ficam só na **interna**.

## 1.4 Maven — `pom.xml` (slides 21–32 produtor; 52–63 consumidor)

O POM é o "modelo de projeto": configurações de build, dependências e plugins. Ref.: https://maven.apache.org/pom.html

**Estrutura (produtor):**
- Cabeçalho: `modelVersion 4.0.0`, namespaces XML.
- Identificação: `groupId = br.com.meslin`, `artifactId = producer-service` (nome do JAR), `version = 1.0-SNAPSHOT`.
- Propriedades: `maven.compiler.source/target = 11` (produtor). No consumidor aparece `17` nas propriedades, mas o `maven-compiler-plugin` força `11` (inconsistência dos slides — ver "Pontos de atenção").
- `<build>`: `finalName` = nome do artefato; `plugins`:
  1. **maven-compiler-plugin 3.8.1** — compila (source/target/release = 11).
  2. **maven-jar-plugin 3.2.0** — com `<phase>none</phase>` na execução `default-jar`: **desativa** o JAR padrão, para gerar só o JAR que queremos.
  3. **maven-assembly-plugin 3.3.0** — gera **`jar-with-dependencies`** (fat JAR) na fase `package`, com `mainClass`:
     - produtor: `br.com.meslin.ChatProducer`
     - consumidor: `br.com.meslin.ChatConsumer`

**Dependências:**

| Dependência | Versão | Uso |
|---|---|---|
| `org.apache.kafka:kafka-clients` | 3.8.0 | Cliente Kafka (produtor e consumidor) |
| `org.glassfish.tyrus:tyrus-server` | 1.13.1 | Servidor WebSocket (referência JSR-356) |
| `org.glassfish.tyrus:tyrus-container-grizzly-server` | 1.13 | Container Grizzly para o Tyrus |
| `org.glassfish.tyrus.bundles:tyrus-standalone-client` | 1.13.1 | Cliente WebSocket (só no produtor) |
| `org.glassfish.tyrus:tyrus-container-grizzly` | 1.2.1 (`scope test` no produtor) | Container cliente |
| `javax.websocket:javax.websocket-api` | 1.1 | API WebSocket (consumidor) |
| `org.slf4j:slf4j-api` + `slf4j-simple` | 2.0.13 (produtor) / 1.7.24 (consumidor) | Logging |

**Build:** `mvn clean package` gera `target/<servico>-jar-with-dependencies.jar`.

## 1.5 Código Java

### `ChatProducer.java` (slides 33–39)
- Imports: `KafkaProducer`, `ProducerRecord`, `ProducerConfig`, `StringSerializer`, `Properties`, `Date`, SLF4J.
- Atributos: `KafkaProducer<String,String> producer`, `String topic`, `Logger`.
- **Construtor** `ChatProducer(String topic)` — configura:
  ```java
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  producer = new KafkaProducer<>(props);
  ```
  (Docs: https://kafka.apache.org/documentation/#producerconfigs)
- `sendMessage(String message)`: prefixa a data (`new Date() + " ==> " + message`) e faz `producer.send(new ProducerRecord<>(topic, message))`.
- `close()`: fecha o produtor.
- `main`: cria `new ChatProducer("chat-messages")`, chama `WebSocketServer.startServer(chatProducer)`, registra *shutdown hook* (`stopServer()` + `close()`) e mantém a aplicação viva com `wait()` num bloco `synchronized`.

### `WebSocketServer.java` — produtor (slides 42–47)
- Anotação `@ServerEndpoint("/ws")`.
- Atributos: `static ChatProducer chatProducer`, `Session session`, `static Set<WebSocketServer> connections = new CopyOnWriteArraySet<>()` (**thread-safe**), `static Server server` (Tyrus).
- `startServer(ChatProducer)`: `new Server("localhost", 8080, "/chat", null, WebSocketServer.class)` → URL final **`ws://host:8080/chat/ws`** (contexto `/chat` + endpoint `/ws`).
- Callbacks JSR-356:
  - `@OnOpen` — guarda a sessão e adiciona a `connections`.
  - `@OnMessage` — `chatProducer.sendMessage(message)` **e** `broadcast(message)`.
  - `@OnClose` — remove de `connections`.
- `broadcast(msg)`: percorre `connections`; envia `new Date() + " --> " + msg` com `session.getBasicRemote().sendText(...)` dentro de `synchronized(client)`; em `IOException` remove o cliente e fecha a sessão.

### `ChatConsumer.java` (slides 64–69)
- Constantes: `TOPIC = "chat-messages"`, `BOOTSTRAP_SERVERS = "kafka:9092"`, `GROUP_ID = "chat-consumer-group"`.
- Propriedades: `bootstrap.servers`, `group.id`, `key.deserializer` e `value.deserializer` = `StringDeserializer`. (Docs: https://kafka.apache.org/documentation/#consumerconfigs)
- Fluxo: `consumer.subscribe(singletonList(TOPIC))` → `WebSocketServer.startServer()` → loop infinito `consumer.poll(Duration.ofMillis(100))` → para cada record: `logger.info(...)` e `WebSocketServer.broadcast(record.value())` → `finally { consumer.close(); }`.

### `WebSocketServer.java` — consumidor (slides 70–75)
- Igual ao do produtor, porém **sem** `ChatProducer`: `startServer()` sem parâmetros e `broadcast` envia o texto puro (sem prefixo de data). `onMessage` só faz `broadcast`.

### HTTP × WebSocket (slide 40)
- **HTTP:** handshake TCP (SYN, SYN-ACK, ACK) → `GET /` → resposta 200 → dados → fecha (FIN/ACK). Requisição/resposta, cliente inicia.
- **WebSocket:** mesmo handshake TCP, depois **canal persistente com mensagens bidirecionais**; qualquer lado pode iniciar o fechamento.

## 1.6 Dockerfile (slides 48–50, 76–78)

```dockerfile
# VERSÃO ANTIGA (marcada como Deprecated no slide)
FROM openjdk:11-jre-slim
# VERSÃO ATUAL (use esta)
FROM eclipse-temurin:11-jre

WORKDIR /app
COPY target/producer-service-jar-with-dependencies.jar producer-service.jar
EXPOSE 8080
CMD ["java", "-jar", "producer-service.jar"]
```
- Base Java 11 JRE; diretório de trabalho `/app`; copia o JAR gerado pelo Maven (na criação da imagem); `EXPOSE` indica a porta provável; `CMD` roda `java -jar ...` ao subir o container.
- Consumidor: igual, trocando por `consumer-service-jar-with-dependencies.jar` / `consumer-service.jar` e `EXPOSE 8081`.
- A imagem `openjdk` no Docker Hub está **deprecated** → usar `eclipse-temurin`.

## 1.7 Frontend (slides 79–87)

- **Versão web** (`index.html`): HTML+JS no mesmo arquivo ("não mostre isso para o professor de Prog Web"). Abre dois WebSockets: `ws://localhost:8080/chat/ws` (produtor, envia) e `ws://localhost:8081/chat/ws` (consumidor, recebe), com duas `div`s (`chatboxproducer`, `chatboxconsumer`), um `<input>` e botão `Send` → `wsProducer.send(message)`.
- **Versão Python** (por causa do Codespaces), pasta `frontend/`, lib `websocket-client`:
  - `chat_sender.py` → conecta em `ws://localhost:8080/chat/ws`; pede usuário e mensagem em loop (digitar `exit` sai); envia JSON `{"user", "message", "timestamp"}` via `ws.send(json.dumps(msg))`. Callbacks: `on_open`, `on_message`, `on_error`, `on_close`; `WebSocketApp(...).run_forever()`.
  - `chat_listener.py` → conecta em `ws://localhost:8081/chat/ws` (ChatConsumer) e só imprime mensagens de broadcast.

## 1.8 Scripts e como executar (slides 88–92)

**`start.sh`:** roda `./stop.sh` → `mvn clean package` no `producer-service/` e no `consumer-service/` (com `check_result` abortando em erro) → `sudo docker compose build` → `sudo docker compose up -d` → `docker ps -a` → `cd frontend && python3 -m http.server --bind 0.0.0.0 8000`.

**`stop.sh`:** `docker ps -a`, `docker stop` e `docker rm` dos containers `chat-app_consumer-service_1`, `chat-app_producer-service_1`, `chat-app_kafka_1`, `chat-app_zookeeper_1`.

**Execução (slide 90) — vários terminais:**
1. Terminal 1: pasta `chat-app` → `./start.sh`
2. Terminal 2: pasta `frontend` → criar/ativar/configurar venv → `python chat_sender.py`
3. Terminal 3: pasta `frontend` → `python chat_listener.py`
4. Repetir 2 e 3 várias vezes (vários usuários).

**Comandos Docker úteis:**

| Ação | Comando |
|---|---|
| Shell dentro do container | `sudo docker exec -it chat-app_consumer-service_1 /bin/bash` |
| Ver containers (UP?) | `sudo docker ps -a` |
| Logs em tempo real | `sudo docker logs -f chat-app_consumer-service_1` |
| Inspecionar | `sudo docker inspect chat-app_consumer-service_1` |
| Remover container | `sudo docker rm <id>` |
| Remover imagem | `sudo docker rmi <id>` |
| Remover imagens sem uso | `sudo docker image prune -a` |
| Listar imagens (`-q` só IDs) | `sudo docker images` |
| Compose | `docker compose build` / `docker compose up -d` |

## 1.9 ⭐ EXERCÍCIO / TRABALHO proposto (slide 98)

> **Baseado no exemplo produtor-consumidor com um Kafka e um Zookeeper, crie um sistema com um *cluster* de Kafka (2 ou 3 brokers).**
> **Verifique se você consegue derrubar um container de um dos elementos do cluster e o sistema continuar funcionando.**

**Dicas do professor:**
- Para cada broker Kafka (ajustar `ID`, hostnames e portas por broker):
  ```
  KAFKA_LISTENERS: INSIDE://0.0.0.0:9092,OUTSIDE://0.0.0.0:29092
  KAFKA_BROKER_ID: 1
  KAFKA_ADVERTISED_LISTENERS: INSIDE://kafka1:9092,OUTSIDE://localhost:29092
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT
  KAFKA_INTER_BROKER_LISTENER_NAME: INSIDE
  KAFKA_LOG_DIRS: /kafka/logs
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 2
  ```
- Para os Pub/Sub (produtor/consumidor): **não usar valores hard-coded; usar variáveis de ambiente** (ex.: `BOOTSTRAP_SERVERS`, tópico, group id lidos de `System.getenv(...)` e passados no `docker-compose.yml`).

**Implicações práticas para o projeto:**
- Os serviços devem apontar para **vários brokers** em `bootstrap.servers` (ex.: `kafka1:9092,kafka2:9092,kafka3:9092`).
- O tópico precisa de **replication factor ≥ 2** para sobreviver à queda de um broker.
- Atenção ao modelo de listeners `INSIDE`/`OUTSIDE` (tráfego interno entre containers × acesso do host).
- Teste de resiliência: `docker stop <broker>` com o chat funcionando e checar se mensagens continuam fluindo.

---

# PARTE 2 — Kubernetes (K8s) — conceitos (v2 e v3, slides 1–27)

## 2.1 O que é e por que existe
- **Kubernetes/K8s** ("K" + 8 letras + "s"): sistema open-source de **orquestração de containers** para automatizar implantação, escalonamento e gerenciamento. Criado pelo Google (anunciado em 6/jun/2014 por Joe Beda, Brendan Burns e Craig McLuckie); hoje mantido pela comunidade, marca da **CNCF**. Nome vem do grego *kubernḗtēs* (timoneiro/piloto).
- **Por quê:** containers resolvem empacotamento e portabilidade, mas em aplicações distribuídas surgem problemas: executar muitos containers, reiniciar os que falham, distribuir tráfego, escalar, atualizar sem interromper o serviço.
- Abstrai a infraestrutura e permite **declarar o estado desejado**.
- **Container** = unidade de execução; **Docker** = ferramenta/ecossistema para criar/executar containers; **K8s** = coordena containers distribuídos em um cluster. **K8s não substitui o container; ele o orquestra.**
- **K8s NÃO é:** sistema operacional, VM, servidor web, banco de dados, nem ferramenta exclusiva para Docker.

## 2.2 Arquitetura

- **Cluster** = conjunto de máquinas (**nodes**), físicas ou virtuais. Componentes se comunicam via **API do Kubernetes**.

| **Control Plane** (decide e mantém o estado) | Função |
|---|---|
| `kube-apiserver` | API central / interface de comunicação com o cluster |
| `etcd` | Armazenamento do estado do cluster |
| `scheduler` | Escolhe em qual node cada Pod roda |
| `controller manager` | Executa controladores que reconciliam o estado desejado |

| **Worker Node** (executa as aplicações) | Função |
|---|---|
| `kubelet` | Agente que garante que os containers rodem nos Pods |
| container runtime | Docker, **containerd** ou **CRI-O** |
| `kube-proxy` | Rede / networking entre Pods |

O diagrama (slide 9) mostra: `kubectl` e a API do provedor de nuvem falando com o(s) **Master(s)** (`cloud-controller-manager`, `kube-controller-manager`, `kube-scheduler`, `kube-apiserver`, `etcd`); **Node 1** e **Node 2** com Pods + `kube-proxy` + container runtime + `kubelet`; um **Load Balancer** na borda do provedor recebendo os usuários finais.

## 2.3 Objetos principais

| Objeto | O que é |
|---|---|
| **Pod** | Menor unidade de execução. Normalmente 1 container; containers do mesmo Pod compartilham rede e podem compartilhar volumes. **São efêmeros** — nunca depender de um Pod específico. |
| **Deployment** | Define *como* a aplicação roda: imagem, nº de réplicas, recursos, env vars, estratégia de atualização. Cria/gerencia **ReplicaSets**, que mantêm os Pods desejados. |
| **Service** | Ponto de acesso **estável** para um conjunto de Pods (IPs de Pods não são identidade permanente). Tipos comuns: **ClusterIP, NodePort, LoadBalancer**. |
| **ConfigMap** | Configurações **não secretas**. |
| **Secret** | Dados sensíveis (credenciais, tokens). **Secret ≠ automaticamente criptografado.** Ambos podem ser usados via variáveis de ambiente ou volumes. |
| **PV / PVC** | `PersistentVolume` (armazenamento do cluster) e `PersistentVolumeClaim` (pedido de armazenamento da aplicação) — dados que sobrevivem à recriação de Pods. |
| **Namespace** | Separação lógica de recursos (organização, isolamento lógico, controle de acesso): dev, teste, produção, equipes. |
| **Labels / Selectors** | Labels = pares chave/valor (`app=web`); selectors selecionam objetos por label. Services e Deployments usam selectors para se relacionar. |

## 2.4 Princípios de funcionamento

- **Reconciliação (estado desejado × atual):** o admin declara o estado desejado; controladores observam o atual; se há diferença, o K8s tenta corrigir. Ex.: desejo 3 réplicas → um Pod morre → outro é criado.
- **Escalonamento horizontal:** aumentar/reduzir nº de Pods (ex.: de 3 até 10 réplicas), manual ou automático. **HPA** (*Horizontal Pod Autoscaler*) usa CPU, memória e outras métricas.
- **Self-healing:** container falhou → runtime pode reiniciar; Pod sumiu → controlador cria outro; workloads podem ser reagendados conforme condições do cluster.
- **Atualizações e rollback:** Deployments permitem trocar versões; **Rolling Update** substitui Pods gradualmente (reduz indisponibilidade); se a nova versão der problema, faz-se **rollback**.

## 2.5 Fluxo de uma aplicação (slide 25)
1. Criar uma imagem de container → 2. Declarar um Deployment → 3. K8s cria os Pods → 4. Um Service dá acesso estável → 5. O scheduler distribui Pods pelos nodes → 6. Controllers mantêm o estado desejado → 7. HPA pode ajustar a capacidade.

## 2.6 `kubectl` (slide 24)
CLI principal para falar com a API do K8s:
```bash
kubectl get pods
kubectl get deployments
kubectl get services
kubectl describe pod <nome>
kubectl logs <nome>
kubectl apply -f arquivo.yaml
```

## 2.7 Próximos passos sugeridos (slide 28 v2 / 63 v3)
Instalar cluster local → criar Pods/Deployments → expor com Services → ConfigMaps/Secrets/volumes → namespaces → **probes** e recursos de CPU/memória → escalonamento e rolling updates → depois: networking, observabilidade e alta disponibilidade.

---

# PARTE 3 — Manifestos YAML do Kubernetes (somente v3, slides 28–62)

## 3.1 Fundamentos
- **YAML:** formato de serialização legível; **indentação define a hierarquia**; extensão `.yaml`/`.yml`. O K8s o usa para a configuração **desejada** dos recursos. Um arquivo YAML usado pelo `kubectl` é um **manifesto** (descreve um ou mais recursos).

### Estrutura básica de todo manifesto
| Campo | Significado |
|---|---|
| `apiVersion` | Versão da API usada. `v1` → Pod, Service, ConfigMap; `apps/v1` → Deployment, ReplicaSet, StatefulSet; `batch/v1` → Job, CronJob; `networking.k8s.io/v1` → Ingress. Determina quais campos são válidos. |
| `kind` | Tipo do objeto (Namespace, Pod, Deployment, Service, ConfigMap, Secret, PersistentVolumeClaim…). |
| `metadata` | Identidade: `name`, `namespace`, `labels` (classificação/seleção), `annotations` (metadados p/ ferramentas e controladores). |
| `spec` | Estado desejado — o conteúdo **depende do `kind`** (Deployment: `replicas`, `selector`, `template`; Service: `selector`, `ports`). |
| `status` | Estado observado; normalmente preenchido pelo próprio K8s. |

## 3.2 Exemplos por recurso (laboratório *SmartFactory_Lab*, namespace `factoryflow`)

### Namespace (`k8s/namespace.yaml`)
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: factoryflow
```
Aplicar: `kubectl apply -f k8s/namespace.yaml`

### Pod (exemplo genérico)
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: web-pod
spec:
  containers:
  - name: web
    image: nginx:latest
    ports:
    - containerPort: 80
  - name: sidecar
    image: busybox:latest
```
- Pod **não** tem `replicas`, `selector` nem `template`; usado geralmente para testes rápidos.
- `spec.containers` é uma **lista**. Campos frequentes: `name`, `image`, `ports`, `env`, `resources`, `volumeMounts`, `readinessProbe`, `livenessProbe`.

### Deployment (`k8s/web.yaml`) — o exemplo completo do lab
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-deployment
  namespace: factoryflow
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1   # máx. de pods indisponíveis durante a atualização
      maxSurge: 1         # máx. de pods extras além do desejado durante a atualização
  selector:
    matchLabels:
      app: web            # quais pods pertencem a este deployment
  template:
    metadata:
      labels:
        app: web          # labels dos pods criados (devem casar com o selector)
    spec:
      containers:
      - name: web
        image: factoryflow-web:v2   # deve ser construída e estar disponível localmente
        imagePullPolicy: Never      # não baixar de registry (imagem local)
        resources:                  # evita o aviso "noisy neighbor"
          requests: { memory: "128Mi", cpu: "100m" }
          limits:   { memory: "256Mi", cpu: "500m" }
        ports:
        - containerPort: 8000
        readinessProbe:             # "está pronto para receber tráfego?"
          httpGet: { path: /api/v1/readiness, port: 8000 }
          initialDelaySeconds: 5
          periodSeconds: 5
        livenessProbe:              # "ainda está vivo?" (senão reinicia)
          httpGet: { path: /api/v1/liveness, port: 8000 }
          initialDelaySeconds: 10
          periodSeconds: 10
```
Pontos-chave:
- `selector.matchLabels` ↔ `template.metadata.labels` **precisam ser compatíveis**.
- `template.spec` contém a definição dos containers.
- `strategy`: **RollingUpdate** (substituição gradual; campos `maxUnavailable`, `maxSurge`) ou **Recreate**.
- Deployment = apps que precisam de réplicas e atualizações controladas.

### Service (`k8s/web.yaml`)
```yaml
apiVersion: v1
kind: Service
metadata:
  name: web-service
  namespace: factoryflow
spec:
  selector:
    app: web          # pods que recebem o tráfego
  ports:
  - port: 80          # porta exposta pelo Service
    targetPort: 8000  # porta no container/Pod
```
- `nodePort` só é usado quando o tipo do Service é `NodePort`.
- **O Service não cria Pods**; apenas direciona tráfego para Pods existentes.

### ConfigMap
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: web-config
  namespace: factoryflow
data:
  WEB_PORT: "8000"
  READINESS_PATH: "/api/v1/readiness"
  LIVENESS_PATH: "/api/v1/liveness"
```

### Secret (`k8s/mysql.yaml`)
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mysql-secret
  namespace: factoryflow
type: Opaque
stringData:
  MYSQL_ROOT_PASSWORD: "root123"
  MYSQL_PASSWORD: "mysql123"
```
- `stringData` aceita texto puro (o K8s converte para base64 ao armazenar).
- O Deployment referencia o Secret **sem** colocar a senha no YAML do container:
  ```yaml
  env:
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: db-secret
        key: PASSWORD
  ```
  Isso **desacopla** a configuração da definição do container.

### PVC + volumes no Pod
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: dados
spec:
  accessModes:
  - ReadWriteOnce     # leitura/escrita exclusiva
  resources:
    requests:
      storage: 5Gi
```
Uso no Pod/Deployment (o volume é declarado em `spec.volumes`; o container monta com `volumeMounts`):
```yaml
volumeMounts:
- name: dados
  mountPath: /var/lib/app
volumes:
- name: dados
  persistentVolumeClaim:
    claimName: dados
```
- O PV representa o armazenamento do cluster; o Pod normalmente referencia o **PVC**; o provisionamento depende do ambiente.

### Job e CronJob (`batch/v1`)
- `Job` = tarefa que **deve terminar** (batch, backups). `CronJob` = cria Jobs segundo uma **agenda**, usando uma especificação de Job como template.

### Ingress (`k8s/ingress.yaml`)
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: factoryflow-ingress
  namespace: factoryflow
spec:
  rules:
  - http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: web-service
            port:
              number: 8000
```
- Descreve regras de entrada **HTTP/HTTPS** (host, paths, Service de backend, TLS). Normalmente exige um **Ingress Controller**.

## 3.3 Organização dos manifestos
- **Labels conectam recursos:** Deployment cria Pods com `labels: app: web`; Service os seleciona com `selector: app: web`.
- **Múltiplos recursos em um arquivo:** separados por `---` (ex.: `kind: Namespace` `---` `kind: Deployment` `---` `kind: Service`).
- **Arquivo único** = conveniente para labs pequenos; **arquivos separados** = melhor organização/manutenção (`namespace.yaml`, `deployment.yaml`, `service.yaml`, `configmap.yaml`, `secret.yaml`, `pvc.yaml`).
- **Ordem lógica** (K8s não exige uma única ordem, mas dependências sugerem): 1) Namespace → 2) ConfigMap/Secret → 3) Storage/PVC → 4) Deployment/StatefulSet → 5) Service → 6) Ingress.

## 3.4 Aplicar e depurar
```bash
kubectl apply -f deployment.yaml     # um manifesto
kubectl apply -f k8s/                # pasta inteira
# arquivo com vários documentos (---) também funciona
```
`apply` declara que o cluster deve **convergir** para o estado descrito.

| Inspecionar | Depurar |
|---|---|
| `kubectl get pods` | `kubectl apply -f arquivo.yaml` |
| `kubectl get deployments` | `kubectl get <recurso>` |
| `kubectl get services` | `kubectl describe <recurso> <nome>` |
| `kubectl get configmaps` | `kubectl get <recurso> <nome> -o yaml` |
| `kubectl get secrets` | `kubectl logs <pod>` |
| `kubectl describe pod <nome>` | `kubectl get events` |
| `kubectl logs <pod>` | `kubectl explain deployment.spec` |

---

# Pontos de atenção (inconsistências/armadilhas nos slides)

Úteis para quem for implementar ou para uma IA que consumir este resumo:

1. **ZooKeeper × KRaft:** o texto diz que o ZooKeeper não é mais necessário (modo KRaft), mas o `docker-compose.yml` ainda o declara. Para o exercício do cluster, o ideal é KRaft; as "dicas" do professor (`KAFKA_BROKER_ID`, `INSIDE/OUTSIDE`) são do estilo **antigo com ZooKeeper** (imagens tipo `wurstmeister`/`confluentinc`). Defina qual abordagem usar e seja consistente.
2. **Comentário errado no slide 16:** fala de mapear `/var/run/docker.sock`, mas o volume real do código é `kafka-data:/var/lib/kafka/data` (e o volume `kafka-data` precisa ser declarado na seção `volumes:` do compose).
3. **Versão do Java:** consumidor declara `maven.compiler.source/target = 17` nas properties, mas o compiler plugin usa `11`, e o Dockerfile usa JRE 11. Padronize em 11.
4. **Versões de SLF4J:** 2.0.13 (produtor) × 1.7.24 (consumidor). Funciona, mas é inconsistente.
5. **Bind do WebSocket:** `new Server("localhost", 8080, ...)` dentro de container escuta só no loopback; para ser acessível de fora do container, normalmente é preciso `0.0.0.0`.
6. **Endereços hard-coded** (`kafka:9092`, tópico, group id): o professor pede explicitamente **variáveis de ambiente** no exercício.
7. **Ingress × Service:** no exemplo, o Service expõe `port: 80` → `targetPort: 8000`, mas o Ingress aponta para `port.number: 8000`. O Ingress deve apontar para a porta **do Service** (80). Atenção ao reproduzir.
8. **Secret:** o exemplo coloca senhas em texto puro no YAML (`root123`). É só laboratório — Secret não é criptografado por padrão; não versionar senhas reais.
9. **`image: nginx:latest` / `apache/kafka:latest`:** `latest` é conveniente em aula, mas não reprodutível; prefira versões fixas.
10. **Imagens locais no K8s:** `imagePullPolicy: Never` exige que a imagem já exista no runtime do cluster local (ex.: minikube/kind precisam carregar a imagem).

---

# Glossário rápido

| Termo | Definição curta |
|---|---|
| Broker | Servidor Kafka que armazena e serve tópicos |
| Tópico | Canal nomeado de eventos no Kafka (`chat-messages`) |
| Producer / Consumer | Quem publica / quem lê mensagens do tópico |
| Consumer group | Grupo de consumidores que dividem as partições de um tópico (`chat-consumer-group`) |
| Replication factor | Nº de cópias de cada partição entre brokers |
| KRaft | Modo do Kafka sem ZooKeeper (controladores próprios) |
| Fat JAR | JAR com todas as dependências (`jar-with-dependencies`) |
| Bridge network | Rede virtual Docker que conecta containers no mesmo host |
| Pod / Deployment / Service | Unidade de execução / gestor de réplicas / acesso estável |
| Probe | Verificação de saúde (readiness = pronto p/ tráfego; liveness = vivo) |
| Manifesto | Arquivo YAML que descreve recursos K8s |
