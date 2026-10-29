# Blueprint de Arquitetura: Transações Distribuídas com o Padrão Saga

[![Architecture](https://img.shields.io/badge/Architecture-Saga%20Pattern%20(Orchestration)-blue.svg)](#)
[![Consistency](https://img.shields.io/badge/Consistency-Eventual%20(BASE)-green.svg)](#)
[![Pattern](https://img.shields.io/badge/Pattern-Transactional%20Outbox%20%2B%20CDC-orange.svg)](#)
[![Resilience](https://img.shields.io/badge/Resilience-Circuit%20Breaker%20%7C%20DLQ-red.svg)](#)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen.svg)](#)

---

## 1. Enquadramento e Cenário de Negócios

Este projeto apresenta a **Blueprint de Arquitetura de Software** para a modernização do ecossistema de checkout de uma empresa de comércio eletrônico de grande porte. A arquitetura migra de uma aplicação monolítica legada centrada em banco de dados único para uma **arquitetura distribuída baseada em microsserviços desacoplados** com padrão *Database-per-Service*.

### Os Três Domínios Críticos

```
   ┌──────────────────┐         ┌──────────────────────┐         ┌─────────────────────┐
   │  Order Service   │         │  Inventory Service   │         │   Payment Service   │
   │  (Ciclo de Vida) │         │ (Reserva de Estoque) │         │ (Gateway Financeiro)│
   └────────┬─────────┘         └──────────┬───────────┘         └──────────┬──────────┘
            │                              │                                │
     [Order DB Privado]          [Inventory DB Privado]            [Payment DB Privado]
```

1. **Order Service:** Responsável por registrar, manter o ciclo de vida dos pedidos (`PENDING`, `CONFIRMED`, `CANCELLED`) e coordenar as fases da transação.
2. **Inventory Service:** Gerencia o estoque de produtos em tempo real, executa reservas temporárias e libera itens em caso de cancelamento.
3. **Payment Service:** Comunica-se com adquirentes e gateways de pagamento externos (Stripe, Adyen, Cielo), processando cobranças sob cartões de crédito e PIX.

### O Desafio da Transação Distribuída
Como cada microsserviço possui seu próprio banco de dados independente, o uso de transações ACID tradicionais via Two-Phase Commit (2PC) é inviável tecnicamente:
* O 2PC bloqueia recursos de rede (*locking protocol*), gerando gargalos de *throughput*.
* Gateways externos de pagamento não suportam protocolos transacionais distribuídos (XA/2PC).
* Falhas transitórias de conectividade causariam indisponibilidade geral da plataforma.

A solução é adotar o modelo de **Consistência Eventual (BASE)** através do **Padrão Saga**, garantindo que operações concluídas com sucesso em etapas iniciais sejam desfeitas por **transações compensatórias** caso etapas posteriores venham a falhar.

---

## 2. Escolha e Fundamentação do Estilo da Saga: Orquestração vs. Coreografia

Para a transação crítica de compra, foi realizada uma avaliação arquitetural rigorosa entre as duas variantes do padrão Saga:

### Comparativo Arquitetural

| Critério Arquitetural | Saga por Coreografia | Saga por Orquestração | Escolha do Projeto |
| :--- | :--- | :--- | :--- |
| **Ponto de Controle** | Descentralizado (orientado a eventos autônomos). | Centralizado no **Order Service** (Saga Orchestrator). | **Orquestração** |
| **Acoplamento Semântico** | **Alto:** Serviços precisam escutar eventos de compensação e regras de domínios vizinhos. | **Baixo:** Serviços participantes tratam apenas seus comandos locais diretos. | **Orquestração** |
| **Visibilidade do Fluxo** | **Fragmentada:** Não existe local único para saber em qual passo o pedido parou. | **Consolidada:** A tabela `saga_instance` detalha o passo exato e histórico da transação. | **Orquestração** |
| **Ordem de Compensação** | Difícil de coordenar em falhas concorrentes. | Determinística e estrita através de pilha **LIFO (Last-In, First-Out)**. | **Orquestração** |
| **Facilidade de Evolução** | Risco de "arquitetura espaguete" com novos participantes (logística, antifraude, NFe). | Linear: basta adicionar novas transições na máquina de estados do orquestrador. | **Orquestração** |

### Justificativa da Decisão

Optou-se explicitamente pela **Saga Baseada em Orquestração**, tendo o **Order Service** como o **Saga Execution Coordinator (SEC)**:
* **Garantia Contábil e Suporte ao Cliente:** Em e-commerce de alto volume, operações de atendimento e conciliação contábil exigem visibilidade instantânea do status do pedido. O orquestrador centraliza essa rastreabilidade sem poluir o domínio dos outros serviços.
* **Desacoplamento Real:** O `Inventory Service` e o `Payment Service` não conhecem a existência um do outro. O inventário não precisa saber que um pagamento falhou; ele apenas atende ao comando `ReleaseStockCommand`.
* **Gerenciamento Seguro de Timeouts:** Permite implementar políticas de prazos (*Deadlines*), *retries* controlados e acionamento cirúrgico de transações compensatórias.

---

## 3. Visão Geral da Arquitetura do Sistema

O diagrama abaixo ilustra a topologia completa de microsserviços, barramento de mensageria assíncrona, bancos de dados dedicados, componentes de resiliência e gateways externos.

![Visão Geral da Arquitetura](diagrams/saga-architecture-overview.png)

### Principais Componentes

1. **API Gateway (Kong / Envoy):** Ponto único de entrada, responsável por autenticação mTLS/JWT, rate limiting e injeção do cabeçalho `X-Trace-ID` / `X-Correlation-ID`.
2. **Order Service (SEC - Saga Execution Coordinator):**
   * Mantém a Máquina de Estados da Saga.
   * Persiste o estado nas tabelas `orders`, `saga_instance` e `saga_step_log`.
   * Publica comandos e escuta eventos de domínio.
3. **Barramento de Mensageria (Apache Kafka):**
   * **Tópicos de Comandos:** `cmd.inventory.reserve`, `cmd.inventory.release`, `cmd.payment.process`.
   * **Tópicos de Eventos:** `evt.inventory.reserved`, `evt.inventory.failed`, `evt.payment.completed`, `evt.payment.failed`.
   * **Dead-Letter Queues (DLQ):** Destinadas ao isolamento de *poison pills* e mensagens com retries esgotados.
4. **Padrão Transactional Outbox + CDC (Debezium):** Elimina o problema de *Dual-Write*. As aplicações gravam alterações e intenções de mensagens atomicamente em seus bancos locais; o Debezium monitora o WAL e despacha para o Kafka com garantia *At-Least-Once*.
5. **Consumidores Idempotentes:** Todos os participantes implementam a tabela de deduplicação `processed_messages` para neutralizar mensagens duplicadas decorrentes de reprocessamento.

---

## 4. Diagrama de Sequência: Fluxo de Sucesso (Happy Path)

O fluxo feliz mapeia a transação completa quando todos os passos locais e chamadas externas são concluídos com êxito.

![Fluxo de Sucesso](diagrams/sequence-success-flow.png)

### Passo a Passo do Fluxo de Sucesso

1. **Criação do Pedido:** O cliente envia uma requisição `POST /api/v1/orders`. O API Gateway injeta o `Correlation-ID` e repassa a chamada.
2. **Persistência Inicial e Resposta Assíncrona:** O `Order Service` abre uma transação ACID local:
   * Insere o pedido com status `PENDING`.
   * Cria o registro no Saga Log (`saga_id`, status `STARTED`).
   * Grava o comando `ReserveStockCommand` na tabela `outbox_events`.
   * Retorna `202 Accepted` imediatamente ao cliente com o `order_id` e `saga_id`.
3. **Despacho do Comando:** O CDC captura a linha do outbox e publica no tópico `cmd.inventory.reserve`.
4. **Reserva de Estoque (Transação Compensável):**
   * O `Inventory Service` consome o comando e valida a chave de idempotência na tabela `processed_messages`.
   * Debita o saldo disponível e credita o saldo reservado (`available_qty -= qty`, `reserved_qty += qty`).
   * Grava o evento `StockReservedEvent` no seu próprio outbox e envia ACK ao Kafka.
5. **Avanço da Saga:**
   * O CDC do inventário publica `StockReservedEvent`.
   * O `Order Service` consome o evento, atualiza o Saga Log (`Step 1 = SUCCESS`) e grava o comando `ProcessPaymentCommand` na tabela outbox.
6. **Cobrança Financeira (Transação Pivot):**
   * O `Payment Service` consome o comando de pagamento com checagem de idempotência.
   * Invoca a API REST externa do Gateway Financeiro (Stripe/Adyen) com timeout estrito e proteção de Circuit Breaker.
   * O Gateway autoriza a transação com código `200 OK`.
   * O `Payment Service` grava o pagamento como `CAPTURED`, insere `PaymentCompletedEvent` no outbox e envia ACK ao Kafka.
7. **Finalização da Saga:**
   * O `Order Service` consome `PaymentCompletedEvent`.
   * Atualiza a Saga para `COMPLETED`.
   * Atualiza o pedido para `CONFIRMED` / `PAID`.
   * Dispara notificação push / WebSocket ao cliente informando o sucesso da compra.

---

## 5. Diagrama de Sequência: Fluxo de Falha e Transações Compensatórias

O fluxo de falha detalha o comportamento do sistema quando o Gateway de Pagamento recusa a cobrança após a reserva de estoque ter sido efetuada.

![Fluxo de Compensação](diagrams/sequence-compensation-flow.png)

### Passo a Passo da Compensação LIFO (Rollback Semântico)

1. **Premissa:** As etapas de criação do pedido e reserva temporária de estoque foram concluídas com sucesso. O orquestrador despachou o comando `ProcessPaymentCommand`.
2. **Recusa no Gateway de Pagamento:**
   * O `Payment Service` submete a cobrança ao gateway externo.
   * O gateway recusa a operação com código de erro de negócio (ex.: `422 Unprocessable Entity - Insufficient Funds` ou cartão inválido).
   * Por se tratar de erro de negócio não-transitório, nenhum retry é acionado.
   * O `Payment Service` grava o status do pagamento como `REJECTED`, insere o evento `PaymentFailedEvent` no seu outbox e envia ACK ao broker.
3. **Gatilho de Compensação no Orquestrador:**
   * O `Order Service` consome `PaymentFailedEvent`.
   * A máquina de estados da Saga transiciona para `COMPENSATING`.
   * O orquestrador consulta o Saga Log (`saga_step_log`) e identifica que o passo de reserva de estoque foi concluído e exige compensação reversa.
   * Insere o comando `ReleaseStockCommand` na tabela outbox.
4. **Execução da Transação Compensatória no Inventário:**
   * O `Inventory Service` consome `ReleaseStockCommand`.
   * Valida a idempotência da mensagem.
   * Reverte a reserva: devolve os itens ao estoque disponível (`available_qty += qty`, `reserved_qty -= qty`) e marca a reserva como `RELEASED`.
   * Grava `StockReleasedEvent` em seu outbox e confirma ACK.
5. **Encerramento da Transação com Cancelamento Auditado:**
   * O `Order Service` consome `StockReleasedEvent`.
   * Marca o passo compensatório como `SUCCESS`.
   * Atualiza o status do pedido para `CANCELLED` (motivo: `PAYMENT_REJECTED`).
   * Finaliza a Saga com status definitivo `FAILED_AND_COMPENSATED`.
   * Notifica o cliente via Push/E-mail informando o motivo da recusa e cancelamento.

---

## 6. Matriz de Idempotência, Consistência e Timeouts de Rede

### 6.1. Padrão Idempotent Consumer

Devido à garantia *At-Least-Once* de brokers de mensageria, uma mensagem pode ser entregue mais de uma vez. Para neutralizar efeitos colaterais indesejados:

* Cada serviço armazena um índice de chaves processadas:
  ```sql
  CREATE TABLE processed_messages (
      message_id VARCHAR(64) PRIMARY KEY,
      saga_id UUID NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
  );
  ```
* A inserção na tabela `processed_messages` ocorre **dentro da mesma transação local ACID** que altera o estado do negócio.
* Caso ocorra violação de unicidade (`ON CONFLICT DO NOTHING`), a operação é descartada silenciosamente e o ACK é enviado ao broker.

### 6.2. Tratamento de Mensagens Fora de Ordem (Semantic Tombstones)

Em cenários anômalos onde uma mensagem compensatória (`ReleaseStockCommand`) ultrapassa o comando original (`ReserveStockCommand`) na rede:
* O `Inventory Service` registra um marcador *Tombstone* com status `PRE_CANCELLED`.
* Quando o comando original de reserva chega atrasado, ele identifica o marcador `PRE_CANCELLED` e cancela a reserva imediatamente sem alocar produtos.

### 6.3. Tabela de Controle de Estado da Saga e Timeouts

```sql
CREATE TABLE saga_instance (
    saga_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    current_step VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL,
    timeout_deadline TIMESTAMP WITH TIME ZONE NOT NULL,
    retry_count INT DEFAULT 0,
    version INT NOT NULL DEFAULT 1
);
```

* **Tratamento de Timeouts:** Se um serviço externo não responder no intervalo limite (`timeout_deadline`), um processo em background (**Saga Sweeper**) consulta a tabela de estado.
* Antes de compensar, o orquestrador executa uma consulta de estado (*Inquiry Pattern*) ao serviço de pagamento para assegurar que a cobrança não foi processada silenciosamente. Se o serviço permanecer mudo, a compensação reversa é ativada.

---

## 7. Estrutura do Repositório

```
saga-architecture-ecommerce/
│
├── README.md                           # Documentação executiva principal
│
├── diagrams/                           # Artefatos visuais de arquitetura
│   ├── saga-architecture-overview.png  # Visão geral de serviços e mensageria
│   ├── saga-architecture-overview.puml # Código-fonte PlantUML da arquitetura
│   ├── sequence-success-flow.png       # Diagrama de sequência do fluxo de sucesso
│   ├── sequence-success-flow.puml      # Código-fonte PlantUML do fluxo de sucesso
│   ├── sequence-compensation-flow.png  # Diagrama de sequência do fluxo de compensação
│   └── sequence-compensation-flow.puml # Código-fonte PlantUML da compensação
│
└── docs/                               # Detalhamento técnico aprofundado
    └── decisoes-arquiteturais.md       # ADRs, riscos mitigados, DLQ e resiliência
```

---

## 8. Como Submeter este Projeto ao GitHub

Para enviar este repositório para o seu perfil no GitHub:

1. Crie um novo repositório vazio no GitHub (ex.: `saga-pattern-ecommerce`).
2. No seu terminal, navegue até a pasta do projeto:
   ```bash
   cd saga-architecture-ecommerce
   ```
3. O repositório remoto já está configurado como `origin`. Para enviar os arquivos:
   ```bash
   git push -u origin main
   ```
