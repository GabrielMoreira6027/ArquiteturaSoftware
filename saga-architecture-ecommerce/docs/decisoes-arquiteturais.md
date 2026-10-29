# Decisões Arquiteturais e Estratégias de Resiliência: Padrão Saga

Este documento compõe a especificação técnica detalhada da arquitetura distribuída para o fluxo crítico de checkout do e-commerce, abordando a fundamentação teórica, as decisões formais de arquitetura (ADRs), os mecanismos de resiliência, a idempotência, o tratamento de falhas e as estratégias de recuperação sob consistência eventual.

---

## 1. Contexto de Engenharia e Desafios de Consistência Distribuída

Na migração do monólito legado para uma arquitetura orientada a serviços desacoplados, a quebra do banco de dados relacional único em bancos de dados privados por serviço (*Database-per-Service*) elimina o suporte a transações ACID tradicionais baseadas em protocolo Two-Phase Commit (2PC).

O protocolo 2PC apresenta sérias desvantagens em ambientes de nuvem modernos:
1. **Bloqueio Síncrono (Blocking Protocol):** Recursos ficam bloqueados durante a fase de preparação (*prepare phase*), degradando severamente o *throughput*.
2. **Ponto Único de Falha:** Se o coordenador cair durante a fase de decisão, os participantes ficam travados em estado de incerteza.
3. **Incompatibilidade com Terceiros:** Gateways de pagamento externos (ex.: Stripe, Adyen, Cielo) não implementam interfaces XA/2PC.

Sob o **Teorema CAP** e o teorema **PACELC**, optou-se por um modelo baseado em **Consistência Eventual (BASE - Basically Available, Soft state, Eventual consistency)**, implementado através do **Padrão Saga** com transações compensatórias.

---

## 2. Architecture Decision Records (ADRs)

---

### ADR-001: Adoção do Padrão Saga Baseado em Orquestração

* **Status:** Aprovado
* **Data:** 29 de Outubro de 2026
* **Decisores:** Arquiteto de Soluções Sênior, Tech Leads de Domínio (Order, Inventory, Payment)

#### Contexto
A execução do pedido envolve três microsserviços desacoplados:
* `Order Service`: Registro do pedido e ciclo de vida.
* `Inventory Service`: Reserva e alocação de estoque.
* `Payment Service`: Cobrança junto ao gateway financeiro externo.

Precisamos escolher entre duas abordagens clássicas de Saga:
1. **Saga por Coreografia:** Serviços reagem de forma autônoma a eventos de domínio emitidos no barramento.
2. **Saga por Orquestração:** Um serviço centralizador (Saga Execution Coordinator - SEC) coordena o fluxo, despachando comandos específicos e escutando eventos de resposta.

#### Matriz de Decisão Arquitetural

| Atributo de Qualidade | Saga por Coreografia | Saga por Orquestração | Avaliação / Justificativa |
| :--- | :--- | :--- | :--- |
| **Acoplamento Semântico** | **Alto (Difuso):** Serviços precisam conhecer eventos de compensação e regras de outros domínios. | **Baixo:** Serviços participantes conhecem apenas seus comandos locais. O acoplamento fica restrito ao orquestrador. | **Vencedor: Orquestração** |
| **Visibilidade e Monitorabilidade** | **Baixa:** O estado está disperso nos bancos de dados de todos os serviços. Rastreamento difícil. | **Excelente:** O orquestrador mantém o estado consolidado da transação em uma única tabela (`saga_instance`). | **Vencedor: Orquestração** |
| **Complexidade Cognitiva** | **Crescimento Exponencial:** Risco iminente de "arquitetura espaguete" à medida que novos passos são adicionados. | **Linear:** Adicionar passos (ex.: antifraude, logística, nota fiscal) exige apenas alterar a máquina de estados no SEC. | **Vencedor: Orquestração** |
| **Recuperação de Falhas (LIFO)** | **Complexa:** Dificuldade em coordenar a ordem correta de compensações concorrentes. | **Determinística:** O orquestrador consulta a pilha de execução e reverte os passos concluídos em ordem estrita (LIFO). | **Vencedor: Orquestração** |
| **Risco de Dependências Cíclicas** | **Alto:** A escuta mútua de eventos entre serviços adjacentes frequentemente cria loops infinitos. | **Nulo:** A comunicação segue a topologia de estrela (Comando $\to$ Resposta). | **Vencedor: Orquestração** |
| **Throughput / Latência** | Ligeiramente menor latência de mensageria em fluxos simples de 2 passos. | Mínimo overhead de coordenação, amplamente mitigado com persistência assíncrona. | **Neutro** |

#### Decisão
Adotar o **Padrão Saga Baseado em Orquestração**, posicionando o `Order Service` como o **Saga Execution Coordinator (SEC)**.

O `Order Service` gerencia a Máquina de Estados Finita (FSM) da transação, utilizando o modelo de Comandos Assíncronos (`cmd.*`) e Eventos de Domínio (`evt.*`) trafegados via Apache Kafka.

#### Consequências
* **Positivas:**
  * Facilidade de auditoria e resposta a clientes (ex.: suporte ao cliente consulta o status exato do pedido em milissegundos).
  * Lógica de compensação centralizada, determinística e facilmente testável com testes de contrato e integração.
  * O `Inventory Service` e o `Payment Service` não sabem da existência um do outro, promovendo o princípio da Responsabilidade Única (SRP).
* **Negativas / Mitigações:**
  * Concentração de lógica de fluxo no `Order Service`. **Mitigação:** Isolar a camada de orquestração (State Machine Engine) da camada de domínio de pedidos, utilizando classes de coordenação dedicadas.

---

### ADR-002: Eliminação de Dual-Write com o Padrão Transactional Outbox

* **Status:** Aprovado
* **Decisores:** Arquiteto de Soluções Sênior

#### Contexto
Quando um microsserviço conclui sua transação local no banco de dados e precisa notificar outros serviços via mensageria, surge o clássico problema de **Dual-Write**:
* Se o serviço salvar no banco e cair antes de publicar a mensagem no Kafka, o restante do sistema nunca saberá da operação (inconsistência permanente).
* Se o serviço publicar no Kafka primeiro e a transação no banco falhar (ex.: constraint violation ou queda de energia), o broker receberá uma notificação de algo que nunca existiu no banco (mensagem fantasma).

#### Decisão
Implementar obrigatoriamente o **Transactional Outbox Pattern** em conjunto com Change Data Capture (CDC via Debezium / Kafka Connect) em todos os serviços participantes (`Order Service`, `Inventory Service`, `Payment Service`).

```
[Aplicação] ──(1. Transação Local ACID)──> [DB: Tabela de Negócio + Tabela Outbox]
                                                         │
                                                  (2. WAL / CDC)
                                                         ▼
                                            [Debezium / Outbox Publisher]
                                                         │
                                                  (3. Publicação Segura)
                                                         ▼
                                              [Tópicos Apache Kafka]
```

#### Regras de Implementação
1. Nenhuma aplicação escreve diretamente no Apache Kafka dentro da requisição HTTP ou thread de negócio.
2. Toda alteração de estado no banco de dados DEVE inserir um registro correspondente na tabela `outbox_events` dentro da **mesma transação ACID local**.
3. Um processo independente de CDC (Debezium) monitora o Write-Ahead Log (WAL do PostgreSQL) da tabela `outbox_events` e publica as mensagens no Kafka com garantia de entrega *At-Least-Once*.

---

### ADR-003: Idempotência de Consumo e Prevenção de Transações Fora de Ordem

* **Status:** Aprovado
* **Decisores:** Arquiteto de Soluções Sênior

#### Contexto
Devido à garantia *At-Least-Once Delivery* do Apache Kafka e aos retries automáticos de rede, os consumidores podem receber a mesma mensagem múltiplas vezes. Sem tratamento adequado:
* Um `ReserveStockCommand` duplicado descontaria estoque duas vezes.
* Um `ProcessPaymentCommand` duplicado cobraria o cartão do cliente duas vezes.

Além disso, em situações de degradação de rede ou rebalanceamento de partições, pode ocorrer uma anomalia de **Mensagens Fora de Ordem (Out-of-Order)**: um comando de compensação (`ReleaseStockCommand`) pode chegar ao consumidor ANTES do comando original de reserva (`ReserveStockCommand`).

#### Decisão

#### 1. Idempotent Consumer Pattern (Deduplicação Atômica)
Cada serviço deve manter uma tabela dedicada de mensagens processadas (`processed_messages`):

```sql
CREATE TABLE processed_messages (
    message_id VARCHAR(64) PRIMARY KEY,
    saga_id UUID NOT NULL,
    operation_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    response_payload JSONB
);
```

**Algoritmo do Consumidor:**
1. Iniciar transação ACID no banco de dados local.
2. Tentar inserir a chave única:
   ```sql
   INSERT INTO processed_messages (message_id, saga_id, operation_name)
   VALUES (:msgId, :sagaId, :operation)
   ON CONFLICT (message_id) DO NOTHING;
   ```
3. Se nenhuma linha foi inserida (`affected_rows == 0`), a mensagem é uma duplicata:
   * A aplicação faz o *Acknowledge* (ACK) imediato ao broker.
   * Não reexecuta nenhum efeito colateral de negócio.
4. Se a linha foi inserida com sucesso:
   * Executa a regra de negócio local.
   * Insere o evento de resposta na tabela `outbox_events`.
   * Executa o `COMMIT` da transação local.

#### 2. Tratamento de Compensações Fora de Ordem (Semantic Tombstone)
Para evitar que uma liberação de estoque ocorra antes da reserva e seja perdida:
* Ao receber um `ReleaseStockCommand` referente a uma reserva que ainda não consta no banco de dados, o `Inventory Service` registra um registro de reserva com status especial `PRE_CANCELLED` (Tombstone).
* Quando o comando atrasado `ReserveStockCommand` finalmente chegar, ele detectará o status `PRE_CANCELLED` e abortará imediatamente a alocação de itens, emitindo `StockReservationAbortedEvent`.

---

### ADR-004: Políticas de Timeout, Resiliência e Dead-Letter Queue (DLQ)

* **Status:** Aprovado
* **Decisores:** Arquiteto de Soluções Sênior

#### Contexto
Falhas em chamadas de rede ou integrações externas podem ser:
1. **Transitórias:** Quedas momentâneas de conexão, soluços de DNS, timeouts de socket.
2. **Não-Transitórias:** Erros de validação de negócio (saldo insuficiente, cartão expirado, produto descontinuado).
3. **Erros Fatais (Poison Pills):** Payloads malformados ou bugs de desserialização que quebram a thread do consumidor repetidamente.

#### Decisão

#### 1. Classificação de Falhas e Política de Retry
* **Falhas de Negócio (ex.: HTTP 422 - Saldo Insuficiente):** NÃO sofrem retry. Disparam imediatamente o evento de falha (`PaymentFailedEvent`) para que o orquestrador inicie a compensação.
* **Falhas Transitórias de Rede (ex.: HTTP 503, Gateway Timeout 504, Connection Reset):**
  * Aplicar **Exponential Backoff com Full Jitter**:
    $$\text{Sleep} = \text{random}(0, \; \min(\text{MaxBackoff}, \; \text{BaseBackoff} \times 2^{\text{attempt}}))$$
    * $\text{BaseBackoff} = 200\text{ ms}$
    * $\text{MaxBackoff} = 3000\text{ ms}$
    * $\text{MaxAttempts} = 3$
* **Circuit Breaker:** Implementado no cliente HTTP do `Payment Service` via Resilience4j ou Polly:
  * Taxa de falha limite (*Failure Rate Threshold*): 50% em janela de 20 chamadas.
  * Tempo em estado aberto (*Open State Duration*): 15 segundos.
  * Chamadas de teste (*Half-Open Probe*): 3 requisições consecutivas.

#### 2. Topologia de Filas de Retry e Dead-Letter Queue (DLQ)
Para mensagens assíncronas do Kafka que falharem após os retries locais, adota-se o padrão **Non-Blocking Retry Topics**:

```
[Tópico Principal: cmd.payment.process]
            │ (Falha transitória)
            ▼
[Tópico Retry 1: cmd.payment.process.retry-5s]
            │ (Nova falha)
            ▼
[Tópico Retry 2: cmd.payment.process.retry-30s]
            │ (Esgotou retries / Poison Pill)
            ▼
[Tópico DLQ: dlq.payment.poison-pill]
            │
      (Alerta PagerDuty/Slack + Dashboard de Reconciliação Manual)
```

---

### ADR-005: Mitigação das Anomalias de Falta de Isolamento (BASE)

* **Status:** Aprovado
* **Decisores:** Arquiteto de Soluções Sênior

#### Contexto
Por padrão, Sagas distribuídas não possuem o atributo **I (Isolation)** do ACID. Isso pode causar:
1. **Leituras Sujas (Dirty Reads):** Outro cliente visualiza um produto como "esgotado" enquanto a compra está em andamento, e depois o estoque é devolvido porque o pagamento falhou.
2. **Atualizações Perdidas (Lost Updates):** Uma saga sobrescreve dados sem levar em consideração as alterações feitas por outra saga em execução concorrente.

#### Decisão e Contramedidas
* **Bloqueio Semântico (Semantic Lock):** O `Inventory Service` nunca debita o estoque físico imediatamente. Ele apenas move itens do saldo `available_quantity` para `reserved_quantity`, associando o registro ao `saga_id`.
* **Visão Pessimista (Pessimistic View):** Consultas de catálogo leem o saldo disponível (`available_quantity`), garantindo que itens em processo de compra não sejam vendidos em duplicidade (*overbooking*).
* **Versionamento Otimista (Optimistic Locking):** Todas as entidades possuem um campo `version INT`. Qualquer atualização concorrente com versão defasada é rejeitada com `OptimisticLockException` e reprocessada pelo consumidor idempotente.

---

## 3. Modelo de Dados e Tabela de Controle da Saga (Saga Log)

O `Order Service` gerencia a instância da saga utilizando as seguintes tabelas relacionais em seu banco de dados PostgreSQL privado:

### 3.1. Tabela de Instância da Saga (`saga_instance`)

```sql
CREATE TABLE saga_instance (
    saga_id UUID PRIMARY KEY,
    order_id UUID NOT NULL UNIQUE,
    current_step VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL,
    payload JSONB NOT NULL,
    compensation_stack JSONB NOT NULL DEFAULT '[]'::jsonb,
    retry_count INT NOT NULL DEFAULT 0,
    timeout_deadline TIMESTAMP WITH TIME ZONE NOT NULL,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_saga_status CHECK (
        status IN (
            'STARTED',
            'STOCK_PENDING',
            'STOCK_RESERVED',
            'PAYMENT_PENDING',
            'COMPLETED',
            'COMPENSATING',
            'FAILED_AND_COMPENSATED',
            'REQUIRES_MANUAL_INTERVENTION'
        )
    )
);

CREATE INDEX idx_saga_timeout ON saga_instance (status, timeout_deadline)
WHERE status IN ('STOCK_PENDING', 'PAYMENT_PENDING', 'COMPENSATING');
```

### 3.2. Tabela de Auditoria de Passos (`saga_step_log`)

```sql
CREATE TABLE saga_step_log (
    step_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_id UUID NOT NULL REFERENCES saga_instance(saga_id) ON DELETE CASCADE,
    step_name VARCHAR(50) NOT NULL,
    action_type VARCHAR(20) NOT NULL, -- 'FORWARD' ou 'COMPENSATION'
    status VARCHAR(30) NOT NULL,      -- 'IN_PROGRESS', 'SUCCESS', 'FAILED'
    error_message TEXT,
    request_payload JSONB,
    response_payload JSONB,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);
```

### 3.3. Tabela do Padrão Outbox (`outbox_events`)

```sql
CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(50) NOT NULL, -- 'Order', 'Inventory', 'Payment'
    aggregate_id VARCHAR(100) NOT NULL,
    topic_name VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_outbox_unprocessed ON outbox_events (created_at) WHERE processed = FALSE;
```

---

## 4. Gerenciamento de Timeouts e Detecção de Falhas de Rede

Em arquiteturas distribuídas, a ausência de resposta (silêncio de rede) é pior do que uma resposta explícita de erro. Se o `Payment Service` ou o Gateway externo demorar 60 segundos ou travar sem responder, o pedido não pode ficar indefinidamente em status `PAYMENT_PENDING`.

### Mecanismo de Deadline e Saga Sweeper Job

1. **Definição de Deadline:**
   Ao transicionar para `PAYMENT_PENDING`, o orquestrador define `timeout_deadline = NOW() + INTERVAL '45 SECONDS'`.
2. **Saga Sweeper Worker:**
   Um agendador distribuído leve (executando via Quartz / ShedLock a cada 5 segundos) consulta:
   ```sql
   SELECT saga_id, order_id, current_step, retry_count
   FROM saga_instance
   WHERE status = 'PAYMENT_PENDING'
     AND timeout_deadline < CURRENT_TIMESTAMP;
   ```
3. **Padrão Inquiry (Consulta Ativa):**
   Antes de disparar compensações precipitadas (o que causaria inconsistência se o gateway debitou o cartão mas o ACK de rede falhou), o orquestrador emite um comando especial `QueryPaymentStatusCommand`:
   * Se o `Payment Service` responder que a transação foi aprovada, o orquestrador apenas avança a Saga para `COMPLETED`.
   * Se responder que a transação falhou ou não existe, o orquestrador marca como falha definitiva e inicia a **Compensação LIFO**.
   * Se o serviço continuar inalcançável após 3 tentativas de consulta, a Saga transiciona para `COMPENSATING`, cancelando a reserva de estoque e enfileirando uma pendência de reconciliação contábil.

---

## 5. Playbook de Operações para Dead-Letter Queues (DLQ)

Quando uma mensagem chega ao tópico `dlq.*.poison-pill`:

1. **Alarme Crítico:** Prometheus dispara alerta via Alertmanager para canal do Slack `#alerts-ecommerce-ops` e PagerDuty.
2. **Inspeção do Payload:** O engenheiro de plantão utiliza a CLI de administração do Kafka ou interface web (AKHQ / Kafka UI) para inspecionar os headers e o payload que causaram o descarte:
   * Header `x-exception-message`: Stack trace do erro original.
   * Header `x-original-topic`: Tópico de origem.
   * Header `x-retry-count`: Número de tentativas efetuadas.
3. **Ações Corretivas:**
   * **Se foi bug de código (ex.: NullPointerException por campo nulo):** Faz-se o hotfix e deploy da correção. Em seguida, aciona-se o script de **Redrive de DLQ** para reinjetar as mensagens no tópico principal.
   * **Se foi inconsistência grave irrecuperável:** O operador executa o comando de compensação manual via painel de administração da Saga, devolvendo o estoque e cancelando o pedido de forma auditada.
