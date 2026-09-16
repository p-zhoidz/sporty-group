# Settlement Trigger Service

This project is a reference implementation of an asynchronous bet-settlement pipeline. It
accepts a final event outcome over HTTP, distributes settlement work through Kafka, delivers
individual settlement commands through RocketMQ, and updates bets idempotently in H2.

The implementation is designed to handle events with millions of bets without loading all
bets into memory. Bets are read in keyset-paginated batches and pages for different events can
be processed in parallel.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the detailed design, delivery guarantees,
failure handling, and scaling model.

## Processing flow

```text
POST /api/v1/event-outcomes
        |
        v
Kafka: event-outcomes
        |
        v
Kafka Streams: validate + deduplicate by eventId
        |
        v
Kafka: settlement-page-tasks
        |
        v
Page workers: read up to 1,000 bets and enqueue the next page
        |
        v
Kafka: bet-settlement-commands
        |
        v
Kafka-to-RocketMQ delivery workers
        |
        v
RocketMQ: bet-settlements
        |
        v
Idempotent UPDATE of the bet status
```

## Prerequisites

- Java 21
- Docker with Docker Compose v2
- `curl` for the command-line verification steps
- A browser for the optional H2 Console verification

The Gradle wrapper is included, so a separate Gradle installation is not required.

Verify the required tools:

```bash
java -version
docker --version
docker compose version
```

## Required host ports

The following TCP ports must be free before the full application is started:

| Port | Component | Purpose |
|---:|---|---|
| `8080` | Spring Boot application | HTTP API and H2 Console |
| `9092` | Kafka | Host-side Kafka client connection |
| `9876` | RocketMQ NameServer | Broker discovery |
| `10909` | RocketMQ Broker | Fast-remoting/VIP channel |
| `10911` | RocketMQ Broker | Main broker remoting channel |

Kafka ports `29092` and `29093` are internal Docker-network ports and do not need to be free
on the host.

On macOS or Linux, check whether any required port is already in use:

```bash
for port in 8080 9092 9876 10909 10911; do
  lsof -nP -iTCP:"$port" -sTCP:LISTEN
done
```

No output means that none of these ports currently has a listening process. Stop the reported
process or change its configuration before continuing.

## Run the complete Kafka and RocketMQ flow

The recommended manual run uses both the `local` and `rocketmq` Spring profiles:

- `local` loads deterministic sample bets into H2 and enables detailed application logs;
- `rocketmq` selects the real RocketMQ producer and consumer.

### 1. Start the infrastructure

From the repository root:

```bash
docker compose up -d
docker compose ps
```

Three containers should be running:

```text
sporty-kafka
sporty-rocketmq-nameserver
sporty-rocketmq-broker
```

If a container exits or the application cannot connect, inspect its logs:

```bash
docker compose logs --tail=100 kafka
docker compose logs --tail=100 rocketmq-nameserver
docker compose logs --tail=100 rocketmq-broker
```

Docker Compose starts Kafka, the RocketMQ NameServer, and the RocketMQ Broker. Kafka data and
RocketMQ data are intentionally local/demo data and are not configured as durable production
storage.

### 2. Start the application

Wait until the broker containers have started, then run:

```bash
./gradlew bootRun --args='--spring.profiles.active=local,rocketmq'
```

Keep this terminal open. The application is ready when the log contains a line similar to:

```text
Started SettlementTriggerApplication
```

At startup, Spring `KafkaAdmin` creates these topics:

```text
event-outcomes
event-outcomes.DLT
settlement-page-tasks
settlement-page-tasks.DLT
bet-settlement-commands
bet-settlement-commands.DLT
```

The RocketMQ Broker creates `bet-settlements` on the first publication because automatic topic
creation is enabled in the local broker configuration.

Optionally verify the Kafka topics from another terminal:

```bash
docker exec sporty-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list
```

### 3. Submit an event outcome

The `local` profile loads 19 sample bets for five events:

| Event ID | Event | Suggested winning competitor |
|---|---|---|
| `event-123` | Team A vs Team B | `team-a` |
| `event-456` | Team C vs Team D | `team-d` |
| `event-789` | Team E vs Team F | `team-e` |
| `event-101` | Team G vs Team H | `team-h` |
| `event-202` | Team I vs Team J | `team-i` |

Submit the outcome for `event-123`:

```bash
curl -i -X POST http://localhost:8080/api/v1/event-outcomes \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "event-123",
    "eventName": "Team A vs Team B",
    "eventWinnerId": "team-a"
  }'
```

Expected response:

```text
HTTP/1.1 202
Content-Type: application/json

{"eventId":"event-123","status":"ACCEPTED"}
```

`202 ACCEPTED` means that Kafka acknowledged the event outcome. Settlement continues
asynchronously; the response does not mean that every bet has already been updated.

Invalid requests with blank required fields return `400 Bad Request`. If Kafka does not
acknowledge the outcome within the configured timeout, the API returns
`503 Service Unavailable`, and the caller may safely retry.

### 4. Verify the settlement result

Open [http://localhost:8080/h2-console](http://localhost:8080/h2-console) and use:

```text
Driver Class: org.h2.Driver
JDBC URL: jdbc:h2:mem:settlement
User Name: sa
Password: leave empty
```

Run:

```sql
SELECT BET_ID, EVENT_ID, EVENT_WINNER_ID, STATUS, SETTLED_AT
FROM BETS
WHERE EVENT_ID = 'event-123'
ORDER BY BET_ID;
```

Expected statuses:

| Bet ID | Selected competitor | Expected status |
|---|---|---|
| `bet-001` | `team-a` | `WON` |
| `bet-002` | `team-b` | `LOST` |
| `bet-003` | `team-a` | `WON` |

All three rows should also have a non-null `SETTLED_AT` value.

### 5. Verify deduplication

Send exactly the same `curl` request again. The API still returns `202 ACCEPTED` because the
new Kafka record was acknowledged, but Kafka Streams does not start a second settlement. The
application log should contain:

```text
[OUTCOME_DUPLICATE_SKIPPED][EVENT_ID: event-123]
```

The bet statuses and `SETTLED_AT` values remain unchanged.

### 6. Follow the processing logs

Application logs use a searchable format:

```text
[UPPER_CASE_EVENT_NAME][FIELD: value][FIELD: value]
```

Search by `EVENT_ID` to follow one event and by `BET_ID` to follow one bet. The successful path
contains events for HTTP receipt, Kafka broker acknowledgement, Streams deduplication, page
loading, settlement-command creation, RocketMQ send/receive, and the database update.

Event-level and page-level stages are logged at `INFO`. Per-bet stages are logged at `DEBUG`
to avoid producing millions of `INFO` records for a large event. The `local` profile enables
application `DEBUG` logs automatically. To enable the same trace without `local`, run:

```bash
LOGGING_LEVEL_COM_SPORTYGROUP_SETTLEMENT=DEBUG \
  ./gradlew bootRun --args='--spring.profiles.active=rocketmq'
```

Malformed messages and non-retryable outcomes are logged at `WARN`; temporary infrastructure
or database failures are logged at `ERROR`.

### 7. Stop the application and infrastructure

Stop the application with `Ctrl+C`, then run:

```bash
docker compose down
```

H2 is an in-memory database, so its contents are lost when the application stops. This is an
accepted limitation of the assignment implementation.

## Reset the local demo

Kafka Streams stores the deduplication state both locally and in a Kafka changelog topic. H2,
however, is recreated on every application start. Therefore, after restarting only the
application, a previously used `eventId` can still be recognized as a duplicate while the new
H2 database contains fresh `PENDING` bets.

To repeat the demo from a completely clean state:

1. Stop the application.
2. Remove the disposable broker containers:

   ```bash
   docker compose down
   ```

3. Remove only this application's local Kafka Streams state:

   ```bash
   rm -rf /tmp/kafka-streams/settlement-outcome-deduplicator
   ```

4. Start the infrastructure and application again using the commands above.

The removal command targets only the local state directory of this demo application. Do not
change the path to a broader directory.

## Spring profiles

| Profile selection | Settlement transport | Intended use |
|---|---|---|
| no transport profile | `LoggingSettlementPublisher` | Demonstrate the Kafka flow without changing bet statuses |
| `rocketmq` | Real RocketMQ producer and consumer | Full manual/integration run |
| `in-memory` | Direct in-process settlement | Automated full-flow tests |
| `local` | Loads sample H2 data and enables application `DEBUG` logs | Add to a manual run |

For a meaningful manual end-to-end verification, use `local,rocketmq`. Running with only
`local` reaches the logging publisher but intentionally leaves the sample bets as `PENDING`.

## Configuration overrides

The most useful environment overrides are:

| Environment variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap address |
| `ROCKETMQ_NAME_SERVER` | `localhost:9876` | RocketMQ NameServer address |
| `INSTANCE_ID` | `local` | Prefix for Kafka transactional producer IDs |
| `LOGGING_LEVEL_COM_SPORTYGROUP_SETTLEMENT` | `INFO` | Application log level |

When multiple application instances use the same Kafka cluster, every instance must have a
unique `INSTANCE_ID`. The Kafka Streams `application-id` must remain
`settlement-outcome-deduplicator`; changing it creates a new consumer application and loses the
existing deduplication identity unless state is deliberately migrated.

## Delivery and failure guarantees

- The API waits for Kafka broker acknowledgement before returning `202`.
- Kafka Streams uses `exactly_once_v2`. Recording a processed `eventId`, consuming the input
  offset, and producing the initial page task are committed atomically.
- Page tasks and settlement commands are produced inside Kafka transactions.
- A malformed outcome is routed directly to `event-outcomes.DLT`.
- Failed page tasks and Kafka settlement commands are retried three times with a one-second
  interval and then published transactionally to their corresponding Kafka DLT.
- The Kafka-to-RocketMQ boundary is at-least-once. If RocketMQ acknowledges a send but the
  Kafka transaction does not commit, the command may be sent again.
- RocketMQ uses `betId` as the message key. Its consumer updates a bet only while its status is
  `PENDING`, making redelivery idempotent.
- Malformed RocketMQ messages and `MISSING`/`CONFLICT` settlement results are logged at `WARN`
  and acknowledged without retry.
- Temporary database or network errors return `RECONSUME_LATER`. After three retries,
  RocketMQ moves the message to `%DLQ%settlement-rocketmq-consumers`.

## Run the automated tests

Docker is not required for the test suite. The full-flow test starts Embedded Kafka, and the
RocketMQ adapter and listener are covered with isolated tests.

```bash
./gradlew test
```

The suite covers:

- winner and loser calculation;
- exact-page and multi-page expansion;
- duplicate outcome handling;
- malformed outcome routing to Kafka DLT;
- idempotent database settlement;
- RocketMQ producer acknowledgement and failure handling;
- RocketMQ consumer success, skip, and retry behavior.

A successful run ends with:

```text
BUILD SUCCESSFUL
```

## Troubleshooting

### A Docker container is not running

```bash
docker compose ps -a
docker compose logs --tail=200 <service-name>
```

Valid service names are `kafka`, `rocketmq-nameserver`, and `rocketmq-broker`.

### The application cannot bind a port

Run the port-check command from the [Required host ports](#required-host-ports) section and
stop the process already listening on that port.

### The API returns `503 Service Unavailable`

Confirm that `sporty-kafka` is running and listening on `localhost:9092`, then inspect the
Kafka and application logs. It is safe to retry the same event because downstream processing
is deduplicated by `eventId`.

### Bets remain `PENDING`

Confirm all of the following:

- the application was started with `local,rocketmq`, not only `local`;
- all RocketMQ containers are running;
- the request uses an `eventId` present in the sample data;
- the event was not already present in a retained Kafka Streams state store;
- the logs contain the expected Kafka and RocketMQ processing stages.

If the event was retained by the state store, follow [Reset the local demo](#reset-the-local-demo).
