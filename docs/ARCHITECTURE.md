# Settlement Trigger Service — Implementation Architecture

Status: implemented. Date: 2026-09-16.

## 1. Goal and Constraints

The service accepts the final event outcome, publishes the mandatory `event-outcomes` message, deduplicates events by `eventId`, scans bets page by page, and creates a separate settlement command for each bet.

Accepted homework constraints:

- one `eventId` has one final outcome; no correction/resettlement;
- after the outcome, no new bets are created for the event;
- loss of H2 data on full restart is acceptable;
- the `bets` table is not extended with orchestration housekeeping fields;
- in the `rocketmq` profile settlement is delivered via RocketMQ; `betId` is used as the business idempotency key;
- Kafka and RocketMQ are not combined into an XA/2PC transaction.

Stack: Java 21, Spring Boot 3.5, Spring Kafka, Kafka Streams, RocketMQ 5.3, Spring Data JPA, H2.

## 2. End-to-End Flow

```text
Client
  │ POST /api/v1/event-outcomes
  ▼
Outcome API
  │ publish key=eventId; wait for broker ack
  ▼
Kafka: event-outcomes
  │
  ▼ exactly_once_v2
Kafka Streams Deduplicator
  │ processed-event-outcomes state store
  ├── eventId already exists ─────────────► DROP
  ├── malformed ──────────────────────────► event-outcomes.DLT
  └── new eventId, key=eventId
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
                                                │ synchronous send; key=betId
                                                ▼
                                      RocketMQ: bet-settlements
                                                │ at-least-once
                                                ▼
                                      RocketMQ SettlementConsumer
                                                │
                                                ▼
                         UPDATE bets SET status=? WHERE status=PENDING
```

## 3. Topics

| Topic | Partitions | Key | Purpose |
|---|---:|---|---|
| `event-outcomes` | 16 | `eventId` | Mandatory input event |
| `settlement-page-tasks` | 32 | `eventId` | Continuation of event keyset scan |
| `bet-settlement-commands` | 32 | `betId` | Settlement of an individual bet |
| `event-outcomes.DLT` | 16 | original key | Invalid outcome |
| `settlement-page-tasks.DLT` | 32 | original key | Unprocessed page task |
| `bet-settlement-commands.DLT` | 32 | original key | Unprocessed settlement command |

RocketMQ uses the `bet-settlements` topic with message key `betId`. After three unsuccessful attempts the RocketMQ consumer places the message into the system DLQ topic `%DLQ%settlement-rocketmq-consumers`.

Spring `KafkaAdmin` creates the Kafka topics from `NewTopic` beans at application startup. Docker Compose brings up the Kafka broker, the RocketMQ NameServer, and the RocketMQ Broker. The RocketMQ topic is created by the broker on first publish. The local Kafka replication factor is 1.

Kafka Streams additionally creates a compacted changelog for the persistent state store `processed-event-outcomes`. The changelog allows state recovery after a restart or partition migration to another instance.

## 4. Processing Stages

### 4.1. HTTP → `event-outcomes`

The API validates the mandatory fields, serializes `EventOutcome`, and executes:

```text
kafkaTemplate.send(event-outcomes, eventId, payload).get(timeout)
```

- broker acknowledged the write — `202 ACCEPTED`;
- Kafka unavailable, timeout, or publish failed — `503 Service Unavailable`;
- the client retries the request after a `503` or a lost HTTP response;
- the first and repeated successful POSTs return `202`; there is no synchronous `200 DUPLICATE`.

There is no outcome DB-outbox. This reduces code and eliminates a separate scheduler, sharding, and relay, but the API cannot accept outcomes while Kafka is unavailable.

### 4.2. Outcome Deduplication

Kafka Streams reads `event-outcomes`, checks the Kafka key and payload, and uses a persistent key-value store:

```text
processed-event-outcomes[eventId] = timestamp
```

For a new `eventId` the topology atomically:

1. writes `eventId` into the state store and its changelog;
2. publishes the initial `SettlementPageTask(afterBetId=null)`;
3. commits the input outcome offset.

`processing.guarantee=exactly_once_v2` makes these actions a single Kafka transaction. On crash before commit, neither the marker, nor the page task, nor the offset is visible. After commit, a repeated record with the same `eventId` is found in the store and dropped. Kafka Streams atomically binds the input offset, the state store changelog, and the output record; a plain `@KafkaListener` with a local `Set` does not provide this guarantee.

The state store keeps `eventId` without TTL. This provides permanent deduplication at the cost of growing the store by one small record per completed event.

A malformed outcome is not retried, since a JSON/validation error is deterministic: the original key and payload are immediately published to `event-outcomes.DLT` in the Streams transaction.

### 4.3. Bet Page Scan

`SettlementPageKafkaListener` reads a page task and requests at most `pageSize` bets:

```sql
WHERE event_id = :eventId
  AND bet_id > :afterBetId
ORDER BY bet_id
LIMIT :pageSize
```

For the first page the `afterBetId` condition is absent. The `(event_id, bet_id)` index is used, so there is no expensive offset pagination.

In a single Kafka transaction the page consumer:

1. publishes a `BetSettlementCommand` for each found bet;
2. for a full page publishes the next task with the last `betId`;
3. commits the input offset together with all output records.

An incomplete or empty page ends the chain. If the bet count equals `N × pageSize`, the last full page creates one more empty verification task.

The next task appears only after the previous one and has `key=eventId`. One large event is read sequentially and keeps only one page in memory; different events are processed in parallel by consumer-group partitions.

### 4.4. Settlement Delivery

Commands have Kafka `key=betId`, so bets of one large event are spread across partitions of the bridge workers. `RocketMqSettlementPublisher` serializes the command, sets the RocketMQ message key to `betId`, and performs a synchronous send waiting for `SEND_OK`.

If RocketMQ did not acknowledge the send, the Kafka listener throws an exception and its inbound Kafka transaction rolls back. If RocketMQ acknowledged the send but the process crashed before the Kafka commit, the command will be published again. There is no shared transaction between the brokers, so the Kafka → RocketMQ boundary has at-least-once semantics.

The RocketMQ consumer calls `BetSettlementService`, which executes:

```sql
UPDATE bets
SET status = :result, settled_at = :now
WHERE bet_id = :betId AND status = 'PENDING'
```

If the DB commit succeeded but RocketMQ did not receive a successful consume result, the command arrives again; the conditional update changes 0 rows. So the final processing also has at-least-once delivery with an idempotent effect. The technical RocketMQ `msgId` is not used for dedup: the stable business key remains `betId`. A repeat with the same result is acknowledged as a duplicate. Malformed message, missing `betId`, and conflicting result are considered unrecoverable: the consumer logs `WARN` and acknowledges the message without retry. An unexpected DB/runtime error returns `RECONSUME_LATER`.

## 5. Transactional Boundaries

| Boundary | Mechanism | Guarantee |
|---|---|---|
| HTTP → `event-outcomes` | wait for broker ack | After `202` the record is accepted by Kafka |
| outcome → dedup store + first task | Kafka Streams `exactly_once_v2` | One output per `eventId` |
| page task → commands + next task | Kafka transaction | Offset and all output records are atomic |
| Kafka command → RocketMQ | sync send + Kafka retry | At-least-once, duplicates possible |
| RocketMQ settlement → H2 | conditional update | At-least-once, idempotent effect |
| poison page/command → DLT | transactional recovery | DLT record and recovered offset are atomic |

Consumers use `isolation.level=read_committed`. The JPA and Kafka transaction managers are separated; XA/2PC is absent. For multiple instances the regular Kafka `transaction-id-prefix` is unique via `INSTANCE_ID`. Kafka Streams uses a shared `application-id` so instances join one Streams application.

## 6. Retry and DLT

Page-task and settlement-command listeners share a `DefaultAfterRollbackProcessor`:

- the original Kafka transaction rolls back;
- three retries with a 1 second interval are performed;
- then `DeadLetterPublishingRecoverer` publishes the record to `<source-topic>.DLT`;
- the DLT publish and the recovered-offset commit run in a new Kafka transaction.

A malformed outcome is handled by the Streams topology itself and routed directly to `event-outcomes.DLT`; retrying a deterministic parsing error makes no sense.

A Kafka settlement-command publish failure to RocketMQ causes a Kafka offset rollback and a regular Kafka retry. After three retries the original command lands in `bet-settlement-commands.DLT`. A transient DB/runtime error of the RocketMQ consumer returns `RECONSUME_LATER`; after three retries the RocketMQ broker moves the message to `%DLQ%settlement-rocketmq-consumers`. Malformed, missing, and conflict are not retried: they are logged at `WARN` and acknowledged.

## 7. Main Edge Cases

```text
Kafka unavailable on POST
  → API returns 503; client retries the request.

Kafka accepted the outcome, HTTP response lost
  → client repeats POST; two records appear in event-outcomes;
    Streams state store creates only one page task.

Streams instance crashes before Kafka commit
  → marker, output, and offset roll back; outcome is read again.

Streams instance crashes after commit
  → marker is restored from changelog; repeated outcome is dropped.

Outcome has invalid JSON or key != payload.eventId
  → record is published to event-outcomes.DLT.

Page DB read temporarily fails
  → Kafka transaction rolls back; page task repeats.

Event contains millions of bets
  → only one page in memory; cursor stored in next Kafka task.

Page/command persistently fails
  → after three retries the record is transactionally moved to the matching DLT.

RocketMQ ack received, Kafka commit failed
  → command is republished to RocketMQ with the same business key=betId.

DB settlement commit done, RocketMQ consume result lost
  → message repeats; UPDATE WHERE status=PENDING becomes a no-op.
```

## 8. Scaling

- Outcome API scales as a stateless HTTP/Kafka producer.
- Deduplicator scales by `event-outcomes` partitions; the state store is partitioned and restored from changelog.
- Page workers scale across events by `settlement-page-tasks` partitions.
- One event produces pages sequentially, so the cursor needs no lock/lease.
- Kafka → RocketMQ bridge workers scale by `bet-settlement-commands` partitions, including commands of one large event.
- RocketMQ consumers with a shared group distribute messages across instances; redelivery is safe thanks to the conditional update.
- Expansion memory is bounded by `O(pageSize)`.

Local H2 remains a single-process profile. Real horizontal scaling of delivery workers requires a shared SQL bets database.

## 9. Package Structure

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
│   ├── messaging    Kafka bridge listener and RocketMQ consumer
│   ├── transport    SettlementPublisher adapters, including RocketMQ
│   └── config       RocketMQ producer/consumer lifecycle and properties
├── bet
│   ├── model        BetEntity, BetStatus, BetProjection
│   ├── repository   BetRepository
│   └── service      query and idempotent settlement services
└── config           Kafka topics, listener transactions, time
```

Repositories are called only from the service of their own feature package. API and messaging layers do not access repositories directly.

## 10. Configuration

```yaml
spring:
  kafka:
    streams:
      application-id: settlement-outcome-deduplicator
      properties:
        processing.guarantee: exactly_once_v2
        replication.factor: 1

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
  rocketmq:
    name-server: localhost:9876
    topic: bet-settlements
    producer-group: settlement-rocketmq-producers
    consumer-group: settlement-rocketmq-consumers
    send-timeout: 10s
    send-retries: 3
    max-reconsume-times: 3
    consumer-enabled: true
```

Settlement transport is selected by Spring profile, not by property:

| Active profile | `SettlementPublisher` | Purpose |
|---|---|---|
| no `rocketmq`/`in-memory` | `LoggingSettlementPublisher` | Safe default without external delivery |
| `rocketmq` | `RocketMqSettlementPublisher` | Real delivery and RocketMQ consumer |
| `in-memory` without `rocketmq` | `InMemorySettlementPublisher` | Isolated full-flow test |

The `local` profile is only responsible for H2 seeding and can be combined with `rocketmq` when needed. If `rocketmq` and `in-memory` are activated together, the RocketMQ adapter is selected.

## 11. Verification

- topology test: two outcomes with one `eventId` create one initial page task;
- topology test: malformed outcome lands in DLT;
- unit: winner/loser decision;
- unit: empty, incomplete, and full page, including continuation;
- JPA integration: repeated settlement does not change the settled bet;
- unit: RocketMQ publisher waits for `SEND_OK` and passes `betId` as business key;
- unit: RocketMQ consumer acknowledges applied/duplicate/malformed/missing and retries a temporary processing error;
- full-flow integration with Embedded Kafka: HTTP → Streams dedup → page tasks → settlement commands → `in-memory` profile → bet statuses, including multiple pages and repeated POST;
- Kafka integration: malformed outcome actually appears in `event-outcomes.DLT`.
