# INF1304 – Distribuição e Concorrência (Prof. Alexandre Meslin, PUC-Rio)
## Resumo da Rodada 1: Threads · Ferramentas de Análise · RPC/RMI/gRPC

> **Fontes:** `05-...-Threads.pdf` (133 slides), `06-...-Ferramentas_de_Análise.pdf` (71 slides), `07-...-RPC-RMI.pdf` (85 slides).
> **Objetivo deste documento:** servir de contexto para outra IA entender *quais tecnologias o professor usa/exige*, *como ele as usa* e *quais exercícios ele propõe*.
> **Observação importante:** estes 3 PDFs **não** mencionam Claude, AWS nem bancos de dados. Eles cobrem concorrência (C e Java), ferramentas de diagnóstico, e comunicação remota (gRPC e Java RMI) empacotada com Docker. Os temas de nuvem/BD devem aparecer nos próximos PDFs.

---

## 0. Mapa rápido de tecnologias

| Tecnologia | Onde aparece | Para que o professor usa |
|---|---|---|
| **C + pthreads** (`<pthread.h>`) | PDF 05 | Criar/sincronizar threads em C |
| **Semáforos POSIX** (`<semaphore.h>`) | PDF 05 | Exclusão mútua e produtor/consumidor (P e V de Dijkstra) |
| **Mutex pthread** | PDF 05 | Exclusão mútua (`pthread_mutex_*`) |
| **Java** (`Thread`, `Runnable`, `synchronized`, `ReentrantLock`) | PDF 05 | Threads e condição de corrida em Java |
| **Java `ExecutorService` / `Executors`** | PDF 05 | Pool de threads |
| **Maven** (`pom.xml`, `mvn clean package/install`) | PDF 05 e 07 | Build de todos os projetos Java |
| **Valgrind** (Memcheck, Helgrind, Callgrind) | PDF 06 | Vazamento de memória e *race conditions* em C |
| **Postman** (+ `postman-echo.com`, `jsonplaceholder.typicode.com`) | PDF 06 | Testar APIs REST/HTTP e automatizar testes em JS |
| **Wireshark** | PDF 06 | Capturar e analisar tráfego (GUI) |
| **tcpdump** | PDF 06 e 07 | Capturar tráfego em linha de comando (inclusive em container) |
| **JMeter** (precisa de Java 11) | PDF 06 | Testes de carga/estresse |
| **gRPC + Protocol Buffers** (Java, Netty) | PDF 07 | RPC moderno, contrato em `.proto` |
| **Java RMI** | PDF 07 | RPC orientado a objetos nativo do Java |
| **Docker / docker-compose** | PDF 07 | Colocar cliente e servidor em containers separados |
| *(Sugeridas, ainda não dadas)* **Grafana + Prometheus**, **cURL** | PDF 06 (slide 3) | "Deveríamos ver também" |

---

# PARTE A: PDF 05 · Threads

## A.1 Conceitos

- **Processo** = contexto + código + dados + stack. O contexto tem parte de programa (registradores, status, SP, PC) e parte de kernel (estruturas de VM, tabela de descritores).
- **Thread**: um processo pode ter várias threads. Cada uma tem **seu próprio** PC, SP/stack, registradores, status e TID; todas **compartilham** código, dados, heap, bibliotecas compartilhadas, arquivos abertos e contexto de kernel.
- **Concorrência:** duas threads são concorrentes se seus fluxos lógicos se sobrepõem no tempo; caso contrário são sequenciais.
- **Threads vs processos:**
  - Semelhanças: fluxo lógico próprio, execução concorrente, sujeitas a troca de contexto.
  - Diferenças: threads compartilham código/dados e são mais leves. Em Linux, ~20k ciclos para criar/disparar um processo contra ~10k para uma thread (processo ≈ 2× mais caro).
- **Prós:** compartilhar dados é fácil (menos overhead), criação/término/troca mais baratos. **Contra:** compartilhar demais gera bugs difíceis (condições de corrida).
- **Estados de uma thread (modelo Java):** `NEW → RUNNABLE → RUNNING`, e daí para `WAITING` (espera notificação/interrupção), `TIMED_WAITING` (sleep/timeout), `BLOCKED` (espera I/O ou lock) e `TERMINATED`.

### Quais variáveis são compartilhadas? (slides 20–25, programa `outraStack.c`)
Uma variável é **compartilhada** se pode ser (ou é) referenciada por mais de uma thread.

| Tipo | Exemplo | Instâncias | Compartilhada? |
|---|---|---|---|
| Global | `char **p;` | 1 | Sim |
| Local automática | `i`, `tid` (main), `id` (thread) | 1 por thread que a declara | Privada (mas *qualquer thread pode ler a stack de outra* via ponteiro) |
| Local `static` | `static int sVar` | 1 para todas | Sim |
| Parâmetro | `vargp` | 1 por thread | Privada |

Moral: "globais são compartilhadas e locais são privadas" **não** é uma regra suficiente. No exemplo, `vMsgs` (local de `main`) acaba compartilhada porque o ponteiro global `p` aponta para ela. Dica do professor: `nm arquivo.o` mostra em que seção cada símbolo cai.

## A.2 pthreads em C

Interface padrão POSIX com ~60 funções. Compilar com `-lpthread`. Para ver as man pages: `sudo apt install manpages-posix-dev`.

```c
int pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                   void *(*start_routine)(void*), void *arg);   // 0 = sucesso, senão nº do erro
int pthread_join(pthread_t th, void **thread_return);           // espera a thread terminar
pthread_t pthread_self(void);                                    // meu ID
// também: pthread_cancel(), pthread_exit()
// exit() termina TODAS as threads; return termina só a atual
```

- `attr` vem de `pthread_attr_init()` (escalonamento, prioridades) e é liberado com `pthread_attr_destroy()`.
- Para passar **vários parâmetros**, o professor empacota numa `struct` alocada com `malloc`, passa o ponteiro como `arg`, e a thread dá `free` (programa `parametro.c`).
- Programas de exemplo: `aloMundo.c` (cria thread, `join`, `sleep(30)`), `5threads.c` (5 threads dormindo, inspecionadas com `ps um`), `outraStack.c`.

## A.3 Condição de corrida e sincronização em C

**Programa `contadorErrado.c`:** 4 threads × 100.000.000 incrementos de `unsigned contagem` global sem proteção. O resultado final é errado e varia a cada execução (ex.: 115.011.299 em vez de 400.000.000). Causa (mostrada em assembly): `contagem++` vira **Load → Update → Store**, e as threads se intercalam entre essas 3 instruções.

### Semáforos (P e V de Dijkstra)
- `P(s)` (*Proberen*, testar): espera até `s>0` e decrementa. `V(s)` (*Verhogen*, incrementar): `s++`. As duas operações são indivisíveis (garantidas por SO + hardware).
- API POSIX (`<semaphore.h>`):
  ```c
  int sem_init(sem_t *sem, int pshared, unsigned value); // pshared=0: só threads do mesmo processo; ≠0: entre processos
  int sem_wait(sem_t *sem);   // = P  (0 sucesso, -1 erro)
  int sem_post(sem_t *sem);   // = V
  ```
- O professor cria *wrappers* `inicializaSemaforo`, `P`, `V` que checam erro e chamam `exit()`.
- **`contadorCerto.c`:** semáforo inicial = 1 em volta de `contagem++` → resultado correto (400.000.000).
- **Custo:** nas medições do slide, a versão sem sincronização termina em ~1 s (mas errada) e a com semáforo leva ~1 min ou mais, com muito tempo de `sys` (leitura minha do slide, que está com o texto embaralhado). Lição: **sincronização correta custa caro**.

### Mutex
```c
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_init(&mutex, attr);   pthread_mutex_destroy(&mutex);
pthread_mutex_lock(&mutex);         // bloqueia na fila se já travado
pthread_mutex_trylock(&mutex);      // NÃO bloqueia se já travado
pthread_mutex_unlock(&mutex);
```
Também citados: `pthread_cond_init()`, `pthread_cond_[timed]wait()` (variáveis condicionais).

### Produtor/consumidor (`produtorConsumidor.c`)
Padrão: produtor insere item no buffer e sinaliza; consumidor espera, retira e sinaliza (aqui "sinalizar" ≠ `signal()`). Implementação com buffer de 1 posição e **dois semáforos**: `vazio` (init 1) e `cheio` (init 0). Produtor: `P(vazio); buffer=item; V(cheio)`. Consumidor: `P(cheio); item=buffer; V(vazio)`. Exemplos reais citados: frames MPEG (produtor) e renderização (consumidor); eventos de UI.

## A.4 Threads em Java (com Maven)

### Estrutura de projeto padrão (repetida em quase todos os exemplos)
- `pom.xml` com `groupId br.com.meslin`, `packaging jar`, **Java 11** (`maven.compiler.source/target`), `maven-compiler-plugin 3.8.1` e `maven-jar-plugin 3.2.0` com `<mainClass>` no manifest.
- Código em `src/main/java/br/com/meslin/...`
- Build e execução:
  ```bash
  mvn clean package
  java -jar target/<artifactId>-1.0-SNAPSHOT.jar
  ```

### Duas formas de criar threads
1. **Estender `Thread`** (`MinhaThread extends Thread`, sobrescreve `run()`).
2. **Implementar `Runnable`** (`MinhaRunnable implements Runnable`) e passar a `new Thread(tarefa)`.
- **Sempre chamar `start()`**, nunca `run()` diretamente.

### Métodos importantes
`start()`, `run()`, `join()`, `isAlive()`, `sleep()`, `interrupt()`, `wait()`, `notify()`.

### Ciclo de vida (`CicloVidaThread.java`)
Demo didática que percorre New → Runnable → Running → Timed Waiting (`Thread.sleep`) → Blocked (tentar entrar em `synchronized(lock)` ocupado) → Waiting (`lock.wait()`, liberada por `lock.notify()`) → Terminated. A thread principal usa `interrupt()`, `sleep` e `join()`.

### Condição de corrida em Java (`Contador.java`)
Duas threads: uma incrementa e outra decrementa o mesmo contador 100.000.000 vezes (deveria terminar em 0). Três variantes:
1. Sem sincronização.
2. **Métodos `synchronized`**.
3. **Blocos `synchronized(lock)`** (também aparece `ReentrantLock` do `java.util.concurrent.locks`).

Resultado do slide: sem sync → **52.581.709** (errado) em **17 ms**; métodos sincronizados → 0 em **6.349 ms**; blocos sincronizados → 0 em **5.433 ms**.

### Observações do professor
- Java **não** oferece memória compartilhada "pura" entre processos como C/C++ (modelo de memória visa segurança/portabilidade).
- Para produtor/consumidor em Java: **`ConcurrentLinkedQueue<Integer>`** (`add` para produzir, `poll` para consumir).

## A.5 Deadlock e starvation

- **Deadlock:** nenhum membro de um grupo consegue prosseguir porque cada um espera outro. As **4 condições necessárias**: (1) exclusão mútua, (2) posse-e-espera, (3) não-preempção, (4) espera circular.
- **Starvation (inanição):** um processo é perpetuamente impedido de obter recursos (pode vir de erro de escalonamento/exclusão mútua, vazamento de recursos ou ataque como *fork bomb*).

## A.6 Thread pool em Java

- Conjunto de threads reutilizáveis: melhora desempenho, gerencia recursos, simplifica a concorrência.
- Classes: `Executors` e interface `ExecutorService` (`execute`, `submit`, `shutdown`, `isTerminated`, `awaitTermination`).
- Tipos: `newFixedThreadPool`, `newCachedThreadPool`, `newScheduledThreadPool`, `newSingleThreadExecutor`.
- **Projeto exemplo `thread-pool`:** calcula (de forma propositalmente lenta) `base^exp` para 1000 pares usando `MyMath.power` (potência por multiplicações sucessivas, que usam somas sucessivas). Um `WorkerThread1 implements Runnable` calcula a posição `index`; `Main` roda com `newFixedThreadPool(5)` e depois sequencialmente, conferindo o resultado contra `BigInteger.pow`.
- **Resultado:** executor ≈ **303.819 ms** vs sequencial ≈ **542.378 ms** (≈1,8× mais rápido).
- **Exercício final do PDF:** implementar um **servidor Web usando pool de threads**. Dica do professor: repositório `github.com/AlexandreMeslin/Distribuicao-Concorrencia`, pasta `web-server`.

## A.7 Exercícios propostos no PDF 05

1. 5 threads iguais: a 1ª imprime "A" a cada 1 s, a 2ª "B" a cada 2 s, a 3ª "C" a cada 3 s, etc.
2. 2 threads (`contCrescente` de 1 a N=20 com pausa T1=1 s; `contDecrescente` de M=30 a 1 com pausa T2=2 s). Depois: (a) variável global incrementada/impressa pelas duas; (b) ler N, M, T1, T2 da linha de comando e **passar por parâmetro** (sem globais).
3. **Produtor/consumidor** com threads: produtor gera inteiro pseudoaleatório a cada 1 s; consumidor retira a cada 2 s; fila de no máximo **8** elementos (`MAXFILA`); ambos produzem/consomem **64** itens; sem condição de corrida (semáforos).
4. **Fumantes** (3 fumantes + 1 fornecedor, com `pthreads` e **variáveis condicionais**; `srand/rand`). Mensagens: `"Fornecedor: disponibilizando x"` (x = Papel, Tabaco ou Fósforo) e `"Fumante y: ah! estou fumando"`.
5. **Jantar dos filósofos** (5 filósofos, 5 garfos, pegar um garfo de cada vez; **como prevenir deadlock**).
6. **Barbeiro dorminhoco** (excesso de clientes, barbeiro dormindo, disputa pela cadeira).

---

# PARTE B: PDF 06 · Ferramentas de Análise

Escopo anunciado: **Valgrind, Postman, Wireshark, VisualVM, tcpdump, Grafana**. No slide 3, "vamos ver": Valgrind, Postman, Wireshark, VisualVM, TCPDump; "deveríamos ver também": Grafana + Prometheus, cURL (e pede sugestões). O conteúdo efetivamente presente é Valgrind, Postman, Wireshark, tcpdump e JMeter (**não há slides de VisualVM nem Grafana neste PDF**).

## B.1 Valgrind

- Framework de **instrumentação dinâmica** (código aberto, GPL v2). Ferramentas: detector de erros de memória (**Memcheck**, padrão), 2 detectores de erros de thread (**Helgrind** e DRD), profilers de cache/branch (Cachegrind, **Callgrind** gera call-graph), 2 profilers de heap, e um gerador SimPoint experimental.
- Plataformas: Linux (x86, AMD64, ARM, ARM64, PPC, S390X, MIPS), Solaris, Android, FreeBSD, macOS (Darwin).
- Instalação: `sudo apt install valgrind` (nos slides: Valgrind 3.18.1).
- Funcionalidades destacadas pelo professor: vazamentos de memória (`malloc/calloc/realloc` sem `free`); erros de memória (variável não inicializada, buffer overflow/underflow, *use-after-free*, *double free*, *mismatched free*); análise de desempenho (Callgrind); problemas de concorrência (Helgrind).

### Casos mostrados (todos em C, rodando `valgrind ./programa`)

| Programa | Defeito | O que o Valgrind reporta |
|---|---|---|
| `alomundo.c` | Nenhum | "All heap blocks were freed -- no leaks are possible", 0 erros |
| `alocacao-v1.c` | `malloc(1000)` sem `free` | LEAK SUMMARY: **definitely lost: 1,000 bytes**. Com `--leak-check=full` mostra a pilha de chamadas da alocação |
| `alocacao-v2.c` | `struct` com ponteiro; faz `pEstrutura = NULL` sem liberar | **definitely lost: 8 bytes** (a struct) + **indirectly lost: 1,000 bytes** (o `pChar` apontado por ela) |
| `vetor-v1.c` | Nenhum (`malloc(sizeof pOrigem)`) | 0 erros |
| `vetor-v2.c` | `malloc(strlen(...))` esquece o `'\0'` | **Invalid write of size 1** (no `strcpy`) e **Invalid read of size 1** (no `puts`), "0 bytes after a block of size 8" |
| `naoInicializado.c` | `if(a)` com `int a` não inicializado | "Conditional jump or move depends on uninitialised value(s)". Com **`--track-origins=yes`** mostra onde o valor nasceu (alocação na stack) |
| `raceCondition.c` | 2 threads fazendo `shared_variable++` (10.000 vezes cada) sem lock | Sem Valgrind, o resultado oscila (20000, 11813, 11665…). Com **`--tool=helgrind`**: "Possible data race during read/write of size 4 … Locks held: none … conflicts with a previous write by thread #2", apontando o símbolo `shared_variable` |

**Comandos-chave:**
```bash
valgrind ./prog                           # Memcheck padrão
valgrind --leak-check=full ./prog         # detalhes dos vazamentos
valgrind --track-origins=yes ./prog       # origem de valores não inicializados
valgrind --tool=helgrind ./prog           # race conditions em programas multithread
# Dica do próprio output: --history-level=approx|none ganha velocidade com menos precisão
```
Compilar com `-g` melhora a saída com arquivo/linha (os slides mostram só endereços, o que sugere compilação sem `-g`).

## B.2 Postman

- Cliente HTTP para **testar serviços Web / APIs REST**; disponível para Linux, Mac e Windows.
- **Instalação:**
  - Windows 10+: baixar o `.exe` e executar.
  - macOS 10.15+: baixar (versão *Apple Chip* se for Apple Silicon), descompactar com o *Archive Utility* padrão, mover para Applications; ou `brew install --cask postman`.
  - Linux (Ubuntu 18.04+, Fedora 32+, Debian 10+): **`snap install postman`** (recomendado) ou baixar e `tar zxf linux_64`. Não rodar com `sudo`; garantir permissão em `~/.config`; instalar `libgconf-2-4` no Ubuntu 18; ter `openssl`. Opção de criar `.desktop` launcher.
- **Interface:** multijanelas/abas; barra de requisição (método + URL + **Send**); abas Params, Authorization, Headers, Body, Pre-request Script, Tests, Settings; no resultado: Body, Cookies, Headers, Test Results e **Status**.
- Uma **requisição HTTP** tem URL, método, headers e body; uma **resposta** tem body, headers e status.
- **Exemplos do professor:**
  1. `GET www.google.com/search?q=Alexandre+Meslin` (parâmetro `q`).
  2. **POST** para `postman-echo.com/post` com body **JSON (raw)**: `{"names": ["Dorothea","Rose","Blanche","Sophia"], "nums": [1,3,2,4]}`.
  3. **Testes automatizados em JavaScript** na aba *Tests*, usando `pm.test`, `pm.response.to.have.status(200)`, `pm.response.to.be.json`, `pm.response.json()` e `pm.expect(...)` (verificar status 200, resposta JSON, `userId == 1`, e que `title` existe e é string). URL usada: `https://jsonplaceholder.typicode.com/posts/1`.
     - ⚠️ O slide diz "Método POST" para esse endpoint, mas `/posts/1` é um recurso de leitura (GET). Possível erro do slide.
- Mais informações: `learning.postman.com`, `docs.postman-echo.com`, `newbookalliance.github.io/API/`.

## B.3 Wireshark

- Analisador de tráfego com **GUI** (antigo Ethereal); parecido com tcpdump, mas com interface gráfica, mais informação e **filtros**. Pode monitorar a máquina local ou, em rede com hub/switch, capturar o que outros transmitem.
- **Interface:** Menu · Lista de pacotes · Detalhes do pacote · Pacote em bytes. Código de cores configurável (ex.: TCP RST em vermelho escuro, HTTP verde, UDP azul, erros de checksum).
- **Menus principais:** `File → Open`, `Capture → Interfaces`, `Analyze → Follow TCP Stream`, `Analyze → Follow HTTP Stream`.
- **Capturar:** (1) escolher a interface com tráfego ativo (Wi-Fi, Ethernet, Bluetooth, `docker0`, `any`, `lo`…); (2) clicar na **barbatana de tubarão azul**; (3) parar no **quadrado vermelho**.
- **Filtros de exibição básicos:** `tcp`, `http`, `ip.addr == 192.168.1.1`, `tcp.port == 80` ou `tcp.port == 443`.
- **Follow TCP Stream:** botão direito em um pacote TCP → *Follow > TCP Stream*; reconstrói a conversa (cliente em vermelho, servidor em azul). Exemplo mostrado: sessão **POP3** (Dovecot) em texto simples.
- **Follow HTTP Stream:** com filtro `http`, botão direito → *Follow > HTTP Stream*; mostra headers (Host, User-Agent), status (200 OK, 404 Not Found) e conteúdo (exemplo com Apache em container Docker).
- **Dicas:** salvar em **`.pcapng`**; **HTTPS é criptografado** (por isso usarão HTTP em sites de teste); o Wireshark captura **todos** os pacotes mesmo os ocultos pelo filtro (cuidado com memória).

## B.4 tcpdump

- Analisador de pacotes em **linha de comando** (licença BSD), usa a biblioteca **libpcap**; roda em Unix-like (Linux, macOS, BSDs…). Port para Windows: **WinDump** (WinPcap).
- Instalação (Debian/Ubuntu):
  ```bash
  sudo apt update
  sudo apt upgrade -y
  sudo apt install tcpdump -y
  ```
- **Exemplo 1: DNS num container/bridge Docker:**
  ```bash
  sudo tcpdump -i br-844bdf0db4d4 -n port 53
  ```
  (`-i` interface, `-n` sem resolver nomes; interface `br-…` é a ponte de uma rede Docker). Mostra consultas `A?`/`AAAA?` e respostas com CNAMEs.
- **Exemplo 2: DHCP:**
  ```bash
  tcpdump -i eth1 -n -e -vvv port 67 or port 68
  ```
  (`-e` mostra cabeçalho Ethernet/MAC, `-vvv` decodificação máxima). Mostra BOOTP/DHCP Discover com opções (hostname, parameter-request…).

## B.5 JMeter (slides finais)

- Ferramenta Apache (projeto Jakarta) para **testes de carga e de estresse** de recursos estáticos ou dinâmicos.
- Conceitos: **Test Plan**, requisições, **assertions**, controladores lógicos (loops, condicionais), **Thread Group** (nº de threads, repetições, intervalo), **listeners** (gráficos/tabelas), componentes.
- Requisito do slide: **Java 11** (`sudo update-alternatives --config java`). A tela mostrada é do Apache JMeter 2.13.

---

# PARTE C: PDF 07 · RPC, gRPC e Java RMI

## C.1 Conceito de RPC

- **Definição:** chamar funções de um processo remoto **como se fossem locais**. **Objetivo:** facilitar a comunicação entre sistemas distribuídos (microsserviços, comunicação entre aplicações).
- **Fluxo (cliente e servidor com stubs):**
  1. Cliente chama o **stub** local.
  2. Stub **serializa** (*marshalling*) os argumentos e envia pela biblioteca RPC/transporte.
  3. Servidor recebe, o stub do servidor **desserializa** (*unmarshalling*) e executa o procedimento.
  4. Resultado volta pelo mesmo caminho (serializa → transporte → stub do cliente desserializa).
- Componentes: Stub Cliente, Stub Servidor, Biblioteca RPC, Protocolo de comunicação, Transporte, Marshalling/Unmarshalling, **Registro** (descoberta de serviço opcional: RMI Registry, DNS, Kubernetes Service).
- **Síncrono** (cliente espera) vs **assíncrono** (cliente segue enquanto espera).
- **Mensagem-chave:** *"O RPC esconde detalhes de comunicação, não elimina seus problemas."* Problemas citados: latência, perda de comunicação, timeout, servidor indisponível, duplicação/retransmissão, falhas parciais, serialização, incompatibilidade de tipos, concorrência no servidor.
- Implementações citadas: **gRPC** (Google, Protocol Buffers, multilinguagem), **Java RMI**, **XML-RPC**, **JSON-RPC**.

## C.2 gRPC

- Pilha: **gRPC → HTTP/2 → TCP → IP → Ethernet/Wi-Fi**. Documentação: `grpc.io/docs`.
- **Arquivo `.proto` = contrato** entre cliente e servidor; **Protocol Buffers** = serialização eficiente de dados estruturados.
- Elementos do `.proto`: `syntax = "proto3";`, `option java_multiple_files = true;`, `package`, `import`, `message` (tipos: `double, float, int32, int64, uint32, uint64, bool, string, bytes`, `repeated`), `service` com `rpc`, comentários.
- **4 tipos de chamada:** **Unário** (1 req → 1 resp), **Server streaming** (1 req → N resp), **Client streaming** (N req → 1 resp), **Bidirecional** (N req → N resp).

### Projeto exemplo `grpc-calculadora`
```
grpc-calculadora/
├── pom.xml
└── src/main
    ├── java/br/com/meslin/calculadora/
    │   ├── client/CalcClient.java
    │   └── server/{CalcServer.java, CalcServiceImp.java}
    └── resources/Calc.proto
```

**`pom.xml`:** Java 8; dependências `io.grpc:grpc-netty`, `grpc-protobuf`, `grpc-stub` (todas **1.36.0**) e `javax.annotation-api 1.2`; extensão **`os-maven-plugin 1.6.2`** (detecta o SO); **`protobuf-maven-plugin 0.6.1`** (`protoc 3.3.0` e `protoc-gen-grpc-java 1.4.0`, `protoSourceRoot = src/main/resources`, goals `compile` e `compile-custom`); **`maven-shade-plugin 3.2.4`** com duas execuções gerando *fat jars* **`grpc-server.jar`** (mainClass `CalcServer`) e **`grpc-client.jar`** (mainClass `CalcClient`).

**`Calc.proto`:**
```proto
syntax = "proto3";
option java_multiple_files = true;
package br.com.meslin;

message Conta          { int32 operando1 = 1; string operacao = 2; int32 operando2 = 3; }
message ContaMultipla  { int32 operando1 = 1; int32 operando2 = 3; }
message Resultado      { int32 resultado = 1; }
message ResultadoMultiplo { int32 resultado = 1; string operacao = 2; }

service CalcService {
  rpc opera(Conta) returns (Resultado);                                   // unário
  rpc operaVariasVezes(ContaMultipla) returns (stream ResultadoMultiplo); // server streaming
}
```

**Fluxo de trabalho:**
1. `mvn clean install` → gera automaticamente as classes em `target/generated-sources/protobuf` (`CalcServiceGrpc`, `Conta`, `ContaMultipla`, `Resultado`, `ResultadoMultiplo`).
2. **Servidor:** `CalcServiceImp extends CalcServiceGrpc.CalcServiceImplBase`; em cada método lê os parâmetros do `request`, calcula, monta o resultado com `Resultado.newBuilder()...build()`, envia com **`responseObserver.onNext(...)`** e fecha com **`responseObserver.onCompleted()`** (no streaming, chama `onNext` uma vez por operação +, −, ×, /). `CalcServer`: `ServerBuilder.forPort(5003).addService(new CalcServiceImp()).build()`, depois `server.start()` e `server.awaitTermination()`.
3. **Cliente:** `ManagedChannelBuilder.forAddress("localhost", 5003).usePlaintext().build()` → `CalcServiceGrpc.newBlockingStub(channel)` → `stub.opera(conta)` (resultado único) e `stub.operaVariasVezes(contaMultipla).forEachRemaining(...)` (stream).
4. Executar: `java -jar target/grpc-server.jar` e `java -jar target/grpc-client.jar`.

**Exercício (gRPC + Docker):** colocar o servidor gRPC (pode ser o da aula) em um container, o cliente em **outro** container, criar um **`docker-compose.yml`** para subir ambos e **mostrar que o cliente acessa o serviço** (o slide termina com "conversação"; texto cortado no PDF).

## C.3 Java RMI

- Definição: API Java que faz *remote method invocation* (equivalente orientado a objetos do RPC), com transferência direta de **classes Java serializadas** e *garbage collection* distribuído.
- **Projeto exemplo `CalculadoraRMI`** (um endpoint por função):
```
CalculadoraRMI/
├── pom.xml
├── client/Dockerfile
├── server/Dockerfile
└── src/main/java/br/com/meslin/calculator/
    ├── client/Client.java
    ├── server/{CalculatorImpl.java, Server.java}
    └── shared/Calculator.java      # (no slide o pacote aparece como "common" em um e "shared" em outros)
```
- **`pom.xml`:** Java 1.8, `maven-shade-plugin 3.2.4` com duas execuções → `rmi-client.jar` (mainClass `...client.Client`) e `rmi-server.jar` (mainClass `...server.Server`).
- **Passos:**
  1. **Interface compartilhada** entre cliente e servidor: `public interface Calculator extends Remote` com `double adicionar/subtrair/multiplicar/dividir(double a, double b) throws RemoteException`.
  2. **Implementação:** `CalculatorImpl extends UnicastRemoteObject implements Calculator` (construtor lança `RemoteException`).
  3. **Servidor:** `LocateRegistry.createRegistry(1099)` (cria o *RMI Registry* na porta **1099**), `Naming.rebind("rmi://localhost/Calculator", calculadora)`.
  4. **Cliente:** `Calculator c = (Calculator) Naming.lookup("rmi://localhost/Calculator");` e chama os métodos como se fossem locais.
- **Build e execução com Docker:**
  ```bash
  mvn clean install
  sudo docker build -t calculator-rmi-server -f server/Dockerfile .
  sudo docker build -t calculator-rmi-client -f client/Dockerfile .
  sudo docker run -d -p 1099:1099 --name rmi-server calculator-rmi-server
  sudo docker run -d --name rmi-client calculator-rmi-client
  sudo docker logs -f rmi-client
  # 12.0 + 8.0 = 20.0 / 12.0 - 8.0 = 4.0 / 12.0 x 8.0 = 96.0 / 12.0 / 8.0 = 1.5
  ```
  *(Nota minha, não do slide: o cliente do exemplo usa `localhost` no `lookup`; com cliente e servidor em containers separados será preciso apontar para o nome/serviço do servidor, p.ex. via rede do docker-compose.)*
- **Segurança:** autenticação pode usar `JMXAuthenticator` ou JMXMP (melhor opção); preferir **SSL/TLS** para criptografar.
- **Vantagens citadas:** facilidade de uso, transparência de localização, reutilização de código, modularidade, escalabilidade.
- **Desafios citados:** concorrência e sincronização, tratamento de falhas, segurança, desempenho.

## C.4 Exercícios propostos no PDF 07

1. **gRPC + Docker:** servidor e cliente em containers distintos + `docker-compose.yml`; demonstrar a comunicação.
2. **RMI + Docker + tcpdump:** `docker-compose.yml` com **cliente RMI** e **servidor RMI**; **analisar a transferência de dados com TCPDump** (usar o container da aula de TCPDump).
3. **Calculadora para números naturais (exercício maior):** reimplementar a calculadora apenas para inteiros positivos com **soma, multiplicação e potenciação**, com as restrições:
   - potenciação usa **multiplicação** para calcular;
   - multiplicação usa **soma** para calcular;
   - **cada função em um local diferente (container)**, ou seja, três serviços independentes que chamam um ao outro remotamente.

---

# PARTE D: Padrões que o professor repete (úteis para orientar o trabalho)

1. **Java + Maven + fat jar** (`maven-shade-plugin`) para cada papel (cliente/servidor) e `java -jar`.
2. **Tudo em container:** cada serviço em seu `Dockerfile`, orquestrados por `docker-compose.yml`; análise de rede com tcpdump/Wireshark sobre as interfaces Docker (`docker0`, `br-...`).
3. **Contrato primeiro:** `.proto` (gRPC) ou interface `Remote` (RMI) compartilhado entre as partes.
4. **Validar concorrência com ferramentas** (Valgrind/Helgrind, medição de tempo com `time` e `System.currentTimeMillis()`), e **testar APIs** com Postman/JMeter.
5. **Mostrar o problema antes da solução** (versão errada → ferramenta detecta → versão corrigida), com medições de desempenho.
6. Preferências do professor: comandos Debian/Ubuntu (`apt`), portas/serviços explícitos (5003 no gRPC, 1099 no RMI), e entregas demonstráveis ("mostre que o cliente consegue acessar o serviço").

---

*Pendências nos PDFs desta rodada:* o PDF 06 anuncia VisualVM, Grafana + Prometheus e cURL mas não traz slides sobre eles; o PDF 07 tem um slide de exercício de gRPC com texto cortado.
