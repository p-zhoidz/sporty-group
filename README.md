# Settlement Trigger Service

Домашняя реализация асинхронного расчёта ставок после получения финального исхода события.
Архитектура и гарантии подробно описаны в [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Запуск

Требования: Java 21 и Docker.

```bash
docker compose up -d
./gradlew bootRun
```

Приложение запускается на `http://localhost:8080`, Kafka — на `localhost:9092`.
Docker Compose запускает только broker. При старте приложения Spring `KafkaAdmin` создаёт
три рабочих топика и три соответствующих DLT-топика.
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

## Основной flow

```text
HTTP → event-outcomes
     → Kafka Streams deduplication by eventId
     → settlement-page-tasks
     → bet-settlement-commands
     → idempotent settlement ставки
```

Outcome-outbox отсутствует: событие считается принятым только после подтверждения Kafka.
Streams использует `exactly_once_v2`, поэтому marker обработанного `eventId`, начальная
page-task и входной offset фиксируются атомарно.

Malformed outcome сразу направляется Streams topology в `event-outcomes.DLT`. Для page-task
и settlement-command выполняются три повтора с интервалом 1 секунду, после чего сообщение
транзакционно переносится в соответствующий DLT:

```text
event-outcomes.DLT
settlement-page-tasks.DLT
bet-settlement-commands.DLT
```

DLT сохраняет key и payload. Основные и DLT-топики объявлены в
`KafkaTopicConfig`, а не создаются Docker Compose.

При нескольких экземплярах Streams `application-id` должен оставаться одинаковым, а
`INSTANCE_ID` обычного Kafka transactional producer — быть уникальным. Локальная in-memory
H2 предназначена для одного процесса; горизонтальный delivery требует общей SQL-базы ставок.

## Тесты

```bash
./gradlew test
```

Набор включает Kafka Streams topology tests, unit-, JPA integration- и параметризованный
full-flow тест с Embedded Kafka, а также проверку реального переноса malformed outcome в DLT.
