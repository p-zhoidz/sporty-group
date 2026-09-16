# Settlement Trigger Service — архитектура реализации

Статус: реализовано. Дата: 2026-09-16.

## 1. Цель и ограничения

Сервис принимает финальный исход события, публикует обязательное сообщение
`event-outcomes`, дедуплицирует события по `eventId`, находит ставки страницами и создаёт
отдельную settlement-команду для каждой ставки.

Принятые ограничения домашнего задания:

- один `eventId` имеет один финальный outcome; correction/resettlement нет;
- после outcome новые ставки на событие не создаются;
- потеря данных H2 при полном перезапуске допустима;
- таблица `bets` не расширяется служебными полями orchestration;
- базовый settlement transport — in-memory adapter; настоящий RocketMQ adapter отсутствует.

Стек: Java 21, Spring Boot 3.5, Spring Kafka, Kafka Streams, Spring Data JPA, H2.

## 2. Сквозной flow

```text
Client
  │ POST /api/v1/event-outcomes
  ▼
Outcome API
  │ publish key=eventId; ждать broker ack
  ▼
Kafka: event-outcomes
  │
  ▼ exactly_once_v2
Kafka Streams Deduplicator
  │ processed-event-outcomes state store
  ├── eventId уже существует ─────────────► DROP
  ├── malformed ──────────────────────────► event-outcomes.DLT
  └── новый eventId, key=eventId
                  ▼
Kafka: settlement-page-tasks
  │ Kafka transaction
  ▼
PageTaskConsumer
  │ DB keyset page
  ├── key=betId ──────────────────────────► bet-settlement-commands
  └── key=eventId ────────────────────────► next settlement-page-task
                                                │
                                                ▼
                                         DeliveryConsumer
                                                │
                                                ▼
                                         SettlementPublisher
                                                │
                                                ▼
                         UPDATE bets SET status=? WHERE status=PENDING
```

## 3. Топики

| Topic | Partitions | Key | Назначение |
|---|---:|---|---|
| `event-outcomes` | 16 | `eventId` | Обязательный входной event |
| `settlement-page-tasks` | 32 | `eventId` | Продолжение keyset-обхода event |
| `bet-settlement-commands` | 32 | `betId` | Settlement отдельной ставки |
| `event-outcomes.DLT` | 16 | исходный key | Невалидный outcome |
| `settlement-page-tasks.DLT` | 32 | исходный key | Необработанная page task |
| `bet-settlement-commands.DLT` | 32 | исходный key | Необработанная settlement-команда |

Топики создаёт Spring `KafkaAdmin` из `NewTopic` beans при старте приложения. Docker Compose
поднимает только Kafka broker. Replication factor локального профиля равен 1.

Kafka Streams дополнительно создаёт compacted changelog для persistent state store
`processed-event-outcomes`. Changelog позволяет восстановить состояние после рестарта или
переноса partition на другой экземпляр.

## 4. Этапы обработки

### 4.1. HTTP → `event-outcomes`

API валидирует обязательные поля, сериализует `EventOutcome` и выполняет:

```text
kafkaTemplate.send(event-outcomes, eventId, payload).get(timeout)
```

- broker подтвердил запись — `202 ACCEPTED`;
- Kafka недоступна, timeout или publish завершился ошибкой — `503 Service Unavailable`;
- клиент повторяет запрос после `503` или потерянного HTTP-response;
- первый и повторный успешные POST возвращают `202`; синхронного `200 DUPLICATE` нет.

Outcome DB-outbox отсутствует. Это уменьшает код и исключает отдельные scheduler,
шардирование и relay, но API не может принимать outcome во время недоступности Kafka.

### 4.2. Дедупликация outcome

Kafka Streams читает `event-outcomes`, проверяет Kafka key и payload и использует persistent
key-value store:

```text
processed-event-outcomes[eventId] = timestamp
```

Для нового `eventId` topology одновременно:

1. записывает `eventId` в state store и его changelog;
2. публикует начальную `SettlementPageTask(afterBetId=null)`;
3. фиксирует offset входного outcome.

`processing.guarantee=exactly_once_v2` делает эти действия одной Kafka-транзакцией. При
падении до commit ни marker, ни page task, ни offset не видны. После commit повторный record
с тем же `eventId` находится в store и отбрасывается. Kafka Streams атомарно связывает
input offset, state store changelog и output record; обычный `@KafkaListener` с локальным
`Set` такой гарантии не даёт.

State store хранит `eventId` без TTL. Это обеспечивает постоянную дедупликацию ценой роста
store на одну небольшую запись для каждого завершённого event.

Malformed outcome не ретраится, поскольку ошибка JSON/валидации детерминирована: исходные
key и payload сразу публикуются в `event-outcomes.DLT` в транзакции Streams.

### 4.3. Постраничный обход ставок

`SettlementPageKafkaListener` читает page task и запрашивает не более `pageSize` ставок:

```sql
WHERE event_id = :eventId
  AND bet_id > :afterBetId
ORDER BY bet_id
LIMIT :pageSize
```

Для первой страницы условие по `afterBetId` отсутствует. Используется индекс
`(event_id, bet_id)`, поэтому нет дорогого offset pagination.

В одной Kafka-транзакции page consumer:

1. публикует `BetSettlementCommand` для найденных ставок;
2. для полной страницы публикует следующую task с последним `betId`;
3. фиксирует входной offset вместе со всеми выходными records.

Неполная или пустая страница завершает цепочку. Если количество ставок равно
`N × pageSize`, последняя полная страница создаёт ещё одну пустую проверочную task.

Следующая task появляется только после предыдущей и имеет `key=eventId`. Один большой event
читается последовательно и держит в памяти только страницу; разные events параллельно
обрабатываются partition-ами consumer group.

### 4.4. Доставка settlement

Команды имеют `key=betId`, поэтому ставки одного большого event распределяются между
partition-ами и delivery workers. Базовый `InMemorySettlementPublisher` вызывает
`BetSettlementService`, выполняющий:

```sql
UPDATE bets
SET status = :result, settled_at = :now
WHERE bet_id = :betId AND status = 'PENDING'
```

Kafka offset и DB commit не являются общей транзакцией. Если DB commit прошёл, а consumer
упал до Kafka commit, команда придёт повторно; conditional update изменит 0 строк. Поэтому
граница Kafka → DB имеет at-least-once delivery с идемпотентным эффектом.

## 5. Транзакционные границы

| Граница | Механизм | Гарантия |
|---|---|---|
| HTTP → `event-outcomes` | ждать broker ack | После `202` record принят Kafka |
| outcome → dedup store + первая task | Kafka Streams `exactly_once_v2` | Один output на `eventId` |
| page task → commands + next task | Kafka transaction | Offset и все output records атомарны |
| settlement command → H2 | conditional update | At-least-once, идемпотентный эффект |
| poison page/command → DLT | transactional recovery | DLT record и recovered offset атомарны |

Consumers используют `isolation.level=read_committed`. JPA и Kafka transaction managers
разделены; XA/2PC отсутствует. Для нескольких экземпляров обычный Kafka
`transaction-id-prefix` уникален через `INSTANCE_ID`. Kafka Streams использует общий
`application-id`, чтобы экземпляры входили в одну Streams application.

## 6. Retry и DLT

Page-task и settlement-command listeners используют общий `DefaultAfterRollbackProcessor`:

- исходная Kafka-транзакция откатывается;
- выполняются три повтора с интервалом 1 секунда;
- затем `DeadLetterPublishingRecoverer` публикует record в `<source-topic>.DLT`;
- DLT publish и фиксация recovered offset выполняются в новой Kafka-транзакции.

Malformed outcome обрабатывает сама Streams topology и сразу направляет в
`event-outcomes.DLT`; повторять детерминированную ошибку парсинга смысла нет.

## 7. Основные edge cases

```text
Kafka недоступна при POST
  → API возвращает 503; клиент повторяет запрос.

Kafka приняла outcome, HTTP-response потерян
  → клиент повторяет POST; в event-outcomes появляются два record;
    Streams state store создаёт только одну page task.

Streams instance падает до Kafka commit
  → marker, output и offset откатываются; outcome читается повторно.

Streams instance падает после commit
  → marker восстанавливается из changelog; повторный outcome отбрасывается.

Outcome имеет невалидный JSON или key != payload.eventId
  → record публикуется в event-outcomes.DLT.

DB read страницы временно падает
  → Kafka transaction откатывается; page task повторяется.

Event содержит миллионы ставок
  → в памяти только одна страница; cursor хранится в следующей Kafka task.

Page/command стабильно не обрабатывается
  → после трёх повторов record транзакционно переносится в соответствующий DLT.

DB commit settlement прошёл, Kafka commit не прошёл
  → команда повторяется; UPDATE WHERE status=PENDING становится no-op.
```

## 8. Масштабирование

- Outcome API масштабируется как stateless HTTP/Kafka producer.
- Deduplicator масштабируется partition-ами `event-outcomes`; state store partitioned и
  восстанавливается из changelog.
- Page workers масштабируются между events partition-ами `settlement-page-tasks`.
- Один event последовательно производит страницы, поэтому cursor не требует lock/lease.
- Delivery workers масштабируются partition-ами `bet-settlement-commands`, включая команды
  одного большого event.
- Память expansion ограничена `O(pageSize)`.

Локальный H2 остаётся одно-процессным профилем. Для реального горизонтального
масштабирования delivery workers нужна общая SQL-база ставок.

## 9. Структура пакетов

```text
com.sportygroup.settlement
├── outcome
│   ├── api          HTTP request/response/controller/error handler
│   ├── model        EventOutcome
│   ├── service      API acceptance and publish error
│   └── messaging    direct Kafka publisher
├── expansion
│   ├── model        SettlementPageTask, SettlementDecider
│   ├── service      page orchestration
│   ├── messaging    page listener and Kafka publishers
│   └── stream       outcome deduplication topology and stateful processor
├── delivery
│   ├── model        BetSettlementCommand
│   ├── service      delivery orchestration
│   ├── messaging    settlement-command listener
│   └── transport    SettlementPublisher adapters
├── bet
│   ├── model        BetEntity, BetStatus, BetProjection
│   ├── repository   BetRepository
│   └── service      query and idempotent settlement services
└── config           Kafka topics, listener transactions, time
```

Repositories вызываются только из service своего feature-пакета. API и messaging слои не
обращаются к repositories напрямую.

## 10. Конфигурация

```yaml
spring:
  kafka:
    streams:
      application-id: settlement-outcome-deduplicator
      properties:
        processing.guarantee: exactly_once_v2

app:
  kafka:
    outcomes-topic: event-outcomes
    page-tasks-topic: settlement-page-tasks
    settlement-commands-topic: bet-settlement-commands
    outcomes-dlt-topic: event-outcomes.DLT
    page-tasks-dlt-topic: settlement-page-tasks.DLT
    settlement-commands-dlt-topic: bet-settlement-commands.DLT
    publish-timeout: 10s
    consumer-retry:
      interval: 1s
      max-retries: 3
  expansion:
    page-size: 1000
    concurrency: 4
  delivery:
    concurrency: 4
    transport: in-memory
```

## 11. Проверки

- topology test: два outcome с одним `eventId` создают одну initial page task;
- topology test: malformed outcome попадает в DLT;
- unit: winner/loser decision;
- unit: пустая, неполная и полная page, включая continuation;
- JPA integration: повторный settlement не меняет рассчитанную ставку;
- full-flow integration с Embedded Kafka: HTTP → Streams dedup → page tasks → settlement
  commands → статусы ставок, включая несколько страниц и повторный POST;
- Kafka integration: malformed outcome реально появляется в `event-outcomes.DLT`.
