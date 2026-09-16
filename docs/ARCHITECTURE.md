# Settlement Trigger Service — архитектура реализации

Статус: реализовано. Дата: 2026-09-16.

## 1. Цель и принятые ограничения

Сервис принимает финальный исход события, надёжно публикует обязательное сообщение
`event-outcomes`, находит все ставки события страницами и передаёт для расчёта отдельную
команду на каждую ставку.

Принятые для домашнего задания ограничения:

- один `eventId` получает только один финальный outcome; correction/resettlement нет;
- после outcome новые ставки на событие не создаются;
- потеря данных H2 при полном перезапуске допустима;
- таблицу `bets` не требуется расширять служебными полями обработки;
- базовый settlement transport — in-memory adapter; RocketMQ можно добавить за интерфейсом
  `SettlementPublisher` без изменения остального flow.

Стек: Java 21, Spring Boot 3.5, Spring Kafka, Spring Data JPA, H2.

Локальный H2-профиль рассчитан на один процесс. Горизонтальное масштабирование API и
outbox relay предполагает общую внешнюю SQL-базу: отдельные in-memory H2 разных JVM не
видят строки друг друга. Это deployment-ограничение, а не дополнительный механизм в коде.

## 2. Базовая схема

```text
Client
  │ POST /api/v1/event-outcomes
  ▼
Outcome API
  │ DB transaction: INSERT event_outbox(event_id, shard_id, payload)
  ▼
Sharded EventOutboxRelay ── key=eventId ──► Kafka: event-outcomes
                                             │ Kafka transaction
                                             ▼
                                    InitialTaskConsumer
                                             │ key=eventId
                                             ▼
                              Kafka: settlement-page-tasks
                                             │ Kafka transaction
                                             ▼
                                     PageTaskConsumer
                                      │ DB keyset page
                                      ├── key=betId ─► Kafka: bet-settlement-commands
                                      └── key=eventId ► next settlement-page-task
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

Назначение топиков:

| Topic | Key | Payload | Назначение |
|---|---|---|---|
| `event-outcomes` | `eventId` | `EventOutcome` | Обязательный входной event по условию задачи |
| `settlement-page-tasks` | `eventId` | `SettlementPageTask` | Последовательное продолжение keyset-обхода одного event |
| `bet-settlement-commands` | `betId` | `BetSettlementCommand` | Параллельная доставка отдельных settlement-команд |
| `<source-topic>.DLT` | исходный key | исходный payload | Невосстановимая ошибка после исчерпания retry |

Основные и DLT-топики создаёт Spring `KafkaAdmin` из `NewTopic` beans при запуске
приложения. Фактическая конфигурация: 16 partitions для `event-outcomes` и его DLT,
32 partitions для page tasks, settlement commands и соответствующих DLT; replication
factor локального профиля равен 1. Совпадающее количество partitions позволяет отправлять
ошибочный record в DLT с сохранением исходного номера partition.

## 3. Детальный flow

### 3.1. Приём outcome

`POST /api/v1/event-outcomes` валидирует запрос и в одной DB-транзакции вставляет строку:

```text
event_outbox(
  event_id PK,
  shard_id,
  payload,
  next_retry_at,
  created_at,
  sent_at NULL
)
```

`shard_id = floorMod(eventId.hashCode(), shardCount)`. PK по `event_id` делает повторный
POST идемпотентным: первый запрос получает `202 ACCEPTED`, повторный — `200 DUPLICATE`.

### 3.2. Шардированный DB-outbox relay

Каждый экземпляр получает `instanceIndex` и `instanceCount` и обслуживает только шарды:

```text
shard % instanceCount == instanceIndex
```

Relay выбирает небольшой batch готовых строк только своих шардов, синхронно ждёт broker ack
для публикации в `event-outcomes`, после чего выставляет `sent_at`. При ошибке переносит
`next_retry_at`.

Это сознательно простая статическая схема:

- разные экземпляры читают непересекающиеся части outbox;
- добавление экземпляров увеличивает параллелизм relay;
- при падении экземпляра его шарды ждут ручного переназначения;
- `shardCount` нельзя менять, пока существуют необработанные строки;
- уникальность `instanceIndex` должна обеспечиваться deployment-конфигурацией.
- все relay instances должны работать с одной общей БД.

Сбой после Kafka publish и до `sent_at` создаёт дубль, но не потерю.

### 3.3. Outcome → первая page task

`OutcomeKafkaListener` читает `event-outcomes`. В Kafka-транзакции он публикует начальную
задачу с `afterBetId = null` в `settlement-page-tasks`. Commit атомарно фиксирует produced
record и consumer offset. При сбое фиксируется ни то, ни другое.

### 3.4. Страничный обход ставок

`SettlementPageKafkaListener` получает одну page task и запрашивает не более `pageSize`
ставок:

```sql
WHERE event_id = :eventId
  AND bet_id > :afterBetId
ORDER BY bet_id
LIMIT :pageSize
```

Для первой страницы условие по `afterBetId` отсутствует. Индекс `(event_id, bet_id)` делает
стоимость запроса зависимой от размера страницы, а не от номера страницы.

В одной Kafka-транзакции consumer:

1. публикует `BetSettlementCommand` для каждой найденной ставки;
2. если получена полная страница, публикует следующую task с последним `betId` как cursor;
3. фиксирует входной offset вместе со всеми выходными records.

Пустая или неполная страница завершает event без отдельной job-таблицы. Для страницы ровно
в `pageSize` записей появится ещё одна, пустая, task — это упрощает протокол и не влияет на
корректность.

Все task одного event имеют `key=eventId`, поэтому попадают в одну Kafka partition. Более
того, следующая task создаётся только при завершении предыдущей: один event обходится
последовательно и с ограниченной памятью, а разные events обрабатываются параллельно
partition-ами и consumer-ами группы.

### 3.5. Доставка settlement

`SettlementCommandKafkaListener` читает команды параллельно. `SettlementPublisher` скрывает
конкретный transport. В базовом профиле `InMemorySettlementPublisher` вызывает
`BetSettlementService`, который выполняет атомарный conditional update:

```sql
UPDATE bets
SET status = :result, settled_at = :now
WHERE bet_id = :betId AND status = 'PENDING'
```

Если DB commit прошёл, а Kafka offset не зафиксирован, команда будет доставлена повторно;
второй update изменит 0 строк. Поэтому граница Kafka → DB имеет at-least-once delivery с
идемпотентным эффектом, а не распределённую exactly-once транзакцию.

## 4. Транзакционные границы и гарантии

| Граница | Механизм | Результат |
|---|---|---|
| HTTP → H2 | DB transaction + outbox | После `202` outcome сохранён, пока живёт H2 |
| H2 outbox → `event-outcomes` | broker ack, затем `sent_at` | At-least-once; возможен дубль |
| `event-outcomes` → page task | Kafka transaction | Offset и task фиксируются атомарно |
| page task → commands + next task | Kafka transaction | Вся страница и cursor-message фиксируются атомарно |
| settlement command → H2 | conditional update | At-least-once с идемпотентным эффектом |

Kafka consumers используют `isolation.level=read_committed`. JPA и Kafka transaction
managers разделены явно: JPA используется для DB-операций, Kafka manager — для consume/
produce стадий. XA/2PC отсутствует.

### 4.1. Retry и DLT

Все три listener используют общий `DefaultAfterRollbackProcessor`. Если listener выбрасывает
исключение, его Kafka-транзакция откатывается. После первоначальной обработки выполняются
три повтора с интервалом 1 секунда. После исчерпания повторов
`DeadLetterPublishingRecoverer` отправляет исходные key и payload в `<source-topic>.DLT`,
сохраняя partition.

Публикация в DLT и фиксация offset проблемного сообщения выполняются в новой
Kafka-транзакции. Поэтому consumer не блокирует partition навсегда и не теряет poison
message: оно становится доступно для отдельного анализа или ручного replay из DLT.

## 5. Edge cases

```text
API DB insert не закоммичен
  → 202 не возвращается; клиент может повторить запрос.

DB commit прошёл, HTTP response потерян
  → повторный POST получает DUPLICATE; новая цепочка не создаётся.

Kafka недоступна для EventOutboxRelay
  → sent_at остаётся NULL; строка повторяется после retry delay.

Outcome опубликован, но markSent не выполнен
  → outcome публикуется повторно; downstream Kafka flow может повториться,
    settlement остаётся безопасным благодаря conditional update.

Outcome/page consumer падает до Kafka commit
  → входной offset и все выходные records откатываются вместе; сообщение читается снова.

DB read страницы временно падает
  → Kafka transaction откатывается; та же page task будет прочитана повторно.

Event содержит миллионы ставок
  → в памяти находится только одна страница; следующий cursor хранится в Kafka message.

Event содержит ровно N × pageSize ставок
  → последняя полная страница создаёт пустую проверочную task, которая завершает цепочку.

Delivery падает до DB commit
  → Kafka offset не фиксируется; команда повторяется.

DB commit прошёл, consumer упал до Kafka offset commit
  → команда повторяется; UPDATE WHERE status=PENDING становится no-op.

Один relay instance недоступен
  → его outbox-шарды не теряются, но стоят до восстановления или переназначения.

Kafka message стабильно не обрабатывается
  → исходная Kafka-транзакция откатывается; после трёх повторов record и ошибка
    публикуются в DLT, а его offset фиксируется в той же recovery-транзакции.
```

## 6. Масштабирование

- Outcome API масштабируется горизонтально; PK `event_outbox.event_id` разрешает гонку
  одинаковых POST.
- EventOutboxRelay масштабируется статическим распределением `shard_id`.
- InitialTaskConsumer масштабируется partition-ами `event-outcomes`.
- PageTaskConsumer масштабируется partition-ами `settlement-page-tasks`; параллелизм — между
  events, но не внутри одного event.
- DeliveryConsumer масштабируется partition-ами `bet-settlement-commands`; один большой
  event может рассчитываться множеством delivery workers после expansion страниц.
- Максимальная память expansion — `O(pageSize)`, а не `O(numberOfBets)`.

Сознательное ограничение: чтение ставок одного event выполняет один page worker за раз.
Это сохраняет простой cursor и порядок без claim/lease таблиц. Параллелизм команд внутри
этого event появляется на следующей стадии.

## 7. Структура пакетов

```text
com.sportygroup.settlement
├── outcome
│   ├── api          HTTP request/response/controller
│   ├── config       shard properties
│   ├── model        EventOutcome, EventOutboxEntity
│   ├── repository   EventOutboxRepository
│   ├── service      acceptance, outbox and shard services
│   └── messaging    sharded relay and Kafka publisher
├── expansion
│   ├── model        SettlementPageTask, SettlementDecider
│   ├── service      initial task and page orchestration
│   └── messaging    Kafka listeners and publishers
├── delivery
│   ├── model        BetSettlementCommand
│   ├── service      delivery orchestration
│   ├── messaging    settlement-command listener
│   └── transport    SettlementPublisher adapters
├── bet
│   ├── model        BetEntity, BetStatus, BetProjection
│   ├── repository   BetRepository
│   └── service      query and idempotent settlement services
└── config           Kafka topics, transactions, scheduling, time
```

Repository вызывается только из service своего feature-пакета. Messaging/API слои работают
с service, а не с repository напрямую.

## 8. Конфигурация

```yaml
app:
  kafka:
    outcomes-topic: event-outcomes
    page-tasks-topic: settlement-page-tasks
    settlement-commands-topic: bet-settlement-commands
    outcomes-dlt-topic: event-outcomes.DLT
    page-tasks-dlt-topic: settlement-page-tasks.DLT
    settlement-commands-dlt-topic: bet-settlement-commands.DLT
    dlt-suffix: .DLT
    consumer-retry:
      interval: 1s
      max-retries: 3
  event-relay:
    shard-count: 16
    instance-count: ${EVENT_RELAY_INSTANCE_COUNT:1}
    instance-index: ${EVENT_RELAY_INSTANCE_INDEX:0}
    batch-size: 100
    poll-interval-ms: 500
  expansion:
    page-size: 1000
    concurrency: 4
  delivery:
    concurrency: 4
    transport: in-memory
```

Число partition должно быть не меньше нужного consumer-параллелизма. Для нескольких
реплик `transaction-id-prefix` Kafka producer обязан быть уникальным (`INSTANCE_ID`).

## 9. Проверки

- unit: winner/loser decision;
- unit: deterministic shard selection и owned shards;
- unit: пустая, неполная и полная page, включая continuation;
- unit: malformed Kafka payload не передаётся в service;
- JPA integration: повторный settlement не меняет рассчитанную ставку;
- full-flow integration с Embedded Kafka: HTTP → sharded outbox → три Kafka стадии →
  итоговые статусы ставок, включая переход через несколько страниц и duplicate POST.
- Kafka integration: malformed outcome после retry попадает в DLT с исходными key/payload.

Пустой event проверяется unit-тестом page service: наблюдаемого DB job-state в упрощённой
архитектуре намеренно нет.
