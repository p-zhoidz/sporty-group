# Settlement Trigger Service

Домашняя реализация асинхронного расчёта ставок после получения финального исхода события.
Архитектура и гарантии подробно описаны в [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Запуск

Требования: Java 21 и Docker.

```bash
docker compose up -d
./gradlew bootRun
```

Приложение запускается на `http://localhost:8080`, Kafka — на `localhost:9092`, RocketMQ
NameServer — на `localhost:9876`. Docker Compose запускает Kafka, RocketMQ NameServer и
RocketMQ Broker. При старте приложения Spring `KafkaAdmin` создаёт три рабочих Kafka-топика
и три соответствующих DLT-топика. RocketMQ Broker автоматически создаёт топик
`bet-settlements` при первой публикации.

Без дополнительного Spring profile settlement-команды выводятся через
`LoggingSettlementPublisher`. Для реальной доставки и обработки через RocketMQ используется
profile `rocketmq`:

```bash
./gradlew bootRun --args='--spring.profiles.active=rocketmq'
```

Profile `in-memory` предназначен для автоматических full-flow тестов и вызывает settlement
ставки напрямую, минуя RocketMQ.

Пример запроса (ставки должны уже присутствовать в таблице `bets`):

```bash
curl -i -X POST http://localhost:8080/api/v1/event-outcomes \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "event-123",
    "eventName": "Team A vs Team B",
    "eventWinnerId": "team-a"
  }'
```

API ждёт broker ack. Успешный запрос возвращает `202 ACCEPTED`, при недоступной Kafka —
`503 Service Unavailable`. Повтор того же `eventId` также возвращает `202`, а downstream
Kafka Streams topology отбрасывает его по persistent state store.

### Локальный запуск с тестовыми ставками

Профиль `local` добавляет в H2 19 ставок для пяти событий:

| Event ID | Участники | Победитель для проверки |
|---|---|---|
| `event-123` | Team A vs Team B | `team-a` |
| `event-456` | Team C vs Team D | `team-d` |
| `event-789` | Team E vs Team F | `team-e` |
| `event-101` | Team G vs Team H | `team-h` |
| `event-202` | Team I vs Team J | `team-i` |

```bash
docker compose up -d
./gradlew bootRun --args='--spring.profiles.active=local,rocketmq'
```

После отправки outcome для нужного `eventId` соответствующие ставки получат статус `WON`
или `LOST`. Результат можно проверить в H2 Console по адресу
`http://localhost:8080/h2-console` со следующими параметрами:

```text
JDBC URL: jdbc:h2:mem:settlement
User Name: sa
Password: <empty>
```

Тестовые данные находятся в `src/main/resources/db/local-data.sql` и не загружаются без
активного профиля `local`.

## Основной flow

```text
HTTP → event-outcomes
     → Kafka Streams deduplication by eventId
     → settlement-page-tasks
     → bet-settlement-commands
     → RocketMQ bet-settlements
     → idempotent settlement ставки
```

Outcome-outbox отсутствует: событие считается принятым только после подтверждения Kafka.
Streams использует `exactly_once_v2`, поэтому marker обработанного `eventId`, начальная
page-task и входной offset фиксируются атомарно.

Malformed outcome сразу направляется Streams topology в `event-outcomes.DLT`. Для page-task
и Kafka settlement-command выполняются три повтора с интервалом 1 секунду, после чего
сообщение транзакционно переносится в соответствующий Kafka DLT:

```text
event-outcomes.DLT
settlement-page-tasks.DLT
bet-settlement-commands.DLT
```

DLT сохраняет key и payload. Основные и DLT-топики объявлены в
`KafkaTopicConfig`, а не создаются Docker Compose. RocketMQ consumer выполняет до трёх
повторов временных ошибок, после чего Broker переносит сообщение в
`%DLQ%settlement-rocketmq-consumers`. Malformed message, отсутствующая ставка или
конфликтующий результат логируются на уровне `WARN` и подтверждаются без повторов.

Граница Kafka → RocketMQ имеет at-least-once semantics: producer ждёт `SEND_OK`, однако при
сбое после RocketMQ ack и до Kafka commit сообщение может быть отправлено повторно. В
RocketMQ message key передаётся `betId`, а consumer применяет условный
`UPDATE ... WHERE status=PENDING`, поэтому повтор имеет идемпотентный эффект.

При нескольких экземплярах Streams `application-id` должен оставаться одинаковым, а
`INSTANCE_ID` обычного Kafka transactional producer — быть уникальным. Локальная in-memory
H2 предназначена для одного процесса; горизонтальный delivery требует общей SQL-базы ставок.

## Тесты

```bash
./gradlew test
```

Набор включает Kafka Streams topology tests, unit-тесты RocketMQ adapter/consumer, JPA
integration- и параметризованный full-flow тест с Embedded Kafka, а также проверку реального
переноса malformed outcome в DLT.
