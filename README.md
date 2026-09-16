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

Первый запрос возвращает `202 ACCEPTED`, повтор того же `eventId` — `200 DUPLICATE`.

## Основной flow

```text
HTTP → sharded event_outbox → event-outcomes
     → settlement-page-tasks → bet-settlement-commands
     → idempotent settlement ставки
```

Для каждого Kafka consumer действует единая recovery policy: первоначальная обработка и
три повтора с интервалом 1 секунду. После этого сообщение транзакционно переносится в DLT:

```text
event-outcomes.DLT
settlement-page-tasks.DLT
bet-settlement-commands.DLT
```

DLT сохраняет key, payload и исходный номер partition. Основные и DLT-топики объявлены в
`KafkaTopicConfig`, а не создаются Docker Compose.

Первый outbox статически распределяется между экземплярами. Такой запуск требует общей
внешней SQL-базы; локальная in-memory H2 предназначена только для одного экземпляра с
`EVENT_RELAY_INSTANCE_COUNT=1`:

```bash
EVENT_RELAY_INSTANCE_COUNT=2 EVENT_RELAY_INSTANCE_INDEX=0 INSTANCE_ID=worker-0 ./gradlew bootRun
EVENT_RELAY_INSTANCE_COUNT=2 EVENT_RELAY_INSTANCE_INDEX=1 INSTANCE_ID=worker-1 ./gradlew bootRun
```

`INSTANCE_ID` должен быть уникальным для Kafka transactional producer каждого экземпляра.

## Тесты

```bash
./gradlew test
```

Набор включает unit-, JPA integration- и параметризованный full-flow тест с Embedded Kafka,
а также проверку реального переноса malformed message в DLT.
