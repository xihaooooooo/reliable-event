# ReliableEvent Kafka example

This standalone Spring Boot example creates orders in its own `reliable_event_kafka_example` MySQL database and registers `order-created` through `ReliableEventPublisher` in the same transaction. The ReliableEvent worker sends the outbox record to Kafka. An example-only native Kafka consumer checks the event identity and order payload, records a transactional deduplication row, applies the order effect, then commits the offset.

The consumer uses `enable.auto.commit=false`. Each record is processed and committed synchronously after the Spring transaction proxy returns. A failed record stays uncommitted; the consumer seeks back to it and pauses briefly before retrying. A duplicate with the same event type, business key, and event ID is skipped. Reusing the business key with another event ID is an error.

Start the isolated MySQL and Kafka services from this directory:

```powershell
docker compose -f docker-compose.yaml up -d
```

Run the application from the repository root after building or installing the reactor:

```powershell
$env:EXAMPLE_KAFKA_CREATE_TOPIC = "true"
mvn -pl reliable-event-kafka-example spring-boot:run
```

Topic creation is an example startup option controlled by `EXAMPLE_KAFKA_CREATE_TOPIC`; the ReliableEvent Kafka producer itself never creates topics. The default example ports are HTTP `8082`, MySQL `13307`, and Kafka `19092`. Post an order with `POST http://localhost:8082/orders`, JSON body `{"itemCode":"book","quantity":2}`, then inspect it with `GET /orders/{id}`. `handledCount` becomes `1` after the consumer transaction commits.

The integration tests use real Kafka and MySQL Testcontainers. Run them with Docker available using `mvn -pl reliable-event-kafka-example test`.
