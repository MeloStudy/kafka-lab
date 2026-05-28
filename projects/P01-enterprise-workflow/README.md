# P01: Enterprise Event-Driven Workflow

## Objective
Build a complete end-to-end flow (Order Processing) that receives an HTTP request, publishes an event, and processes it using **Reactive Programming (Spring WebFlux & Project Reactor)**, core Java clients, and TDD.

## 1. Infrastructure Dissection
The provided `docker-compose.yml` provisions a single-node Apache Kafka cluster using KRaft (KIP-500), eliminating the need for ZooKeeper.
- `KAFKA_CFG_PROCESS_ROLES=controller,broker`: The node acts as both the metadata controller and the message broker.
- `KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093`: Defines the network interfaces.

Start the infrastructure:
```bash
docker-compose up -d
```

## 2. Command Dissection: Topics
Before running the application, let's observe the topics being created or create them manually.
```bash
docker exec -it p01-kafka \
  kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders.inbound \
  --partitions 3 \
  --replication-factor 1
```
- `docker exec -it p01-kafka`: Executes a command inside the Kafka container.
- `kafka-topics.sh`: The native script to manage topics.
- `--create --topic orders.inbound`: Specifies the action and the topic name.
- `--partitions 3`: Distributes the topic across 3 partitions for parallelism.

## 3. Running the Application
Ensure the tests pass first using TDD:
```bash
mvn clean test
```

Run the Spring Boot application:
```bash
mvn spring-boot:run
```

## 4. The Learner Journey (Simulation)
1. Send an HTTP POST to create an order:
```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "O-999", "userId": "U-123", "amount": 500.00}'
```
2. You will receive an immediate `202 Accepted`.
3. In the console logs, observe the `OrderProducer` sending the event, and the `OrderConsumer` processing it and producing to `orders.confirmed`.

## 5. Self-Assessment
<details>
<summary>Why do we return HTTP 202 instead of HTTP 200?</summary>
HTTP 202 (Accepted) indicates that the request has been accepted for processing, but the processing has not been completed. This perfectly matches the asynchronous nature of producing an event to Kafka, where the final confirmation happens eventually in a consumer.
</details>

<details>
<summary>How does `reactor-kafka` handle backpressure?</summary>
It uses Reactive Streams semantics. The consumer pulls records using `poll()` only when the downstream subscribers signal demand. This prevents the application from consuming more messages than it can process in memory.
</details>
