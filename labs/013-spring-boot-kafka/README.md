# LAB-013: Spring Boot & Kafka

In this lab, we integrate Kafka natively with the Spring Boot framework, allowing us to build production-grade event-driven microservices with drastically less boilerplate.

Read [CONCEPT.md](CONCEPT.md) for theory on `KafkaTemplate`, `@KafkaListener`, and `ErrorHandlingDeserializer`.

## Step 1: Run Infrastructure
Spin up the local KRaft cluster:
```bash
docker-compose up -d
```

Create the topic:
```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh \
  --create --bootstrap-server localhost:9092 --topic payments --partitions 3
```

## Step 2: Test the Application
We are using **Testcontainers**. Testcontainers spins up a real Kafka Docker container automatically, runs the test, and tears it down. This ensures your code works against a real broker.

Run the test:
```bash
mvn clean test
```
Notice how it automatically downloads and spins up a temporary Kafka container.

## Step 3: Run the Spring Boot App
Start the Spring Boot application:
```bash
mvn spring-boot:run
```
The app will connect to `localhost:9092` and the `PaymentConsumer` will start listening on the `payments` topic.

## Step 4: Test JSON Deserialization
Let's send a valid JSON message to the topic using the console producer:
```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic payments
```
Paste this valid JSON:
```json
{"id":"P-999","userId":"U-777","amount":50.00,"currency":"EUR"}
```
Check the Spring Boot console. You should see the message successfully converted into a `Payment` object and logged by the consumer.

### Step 5: Test the ErrorHandlingDeserializer (Resilience)
Now, paste this **invalid** JSON:
```text
This is not JSON!
```
Watch the Spring Boot console. Instead of crashing in an infinite loop, the `ErrorHandlingDeserializer` catches the `SerializationException` and logs it, skipping the bad payload while keeping the consumer alive and polling for the next message.

## Self-Assessment
<details>
<summary>1. What is the role of <code>KafkaTemplate</code> compared to the native <code>KafkaProducer</code>?</summary>
<code>KafkaTemplate</code> wraps the native <code>KafkaProducer</code> and provides high-level convenience methods. It abstracts away serialization logic (by reading from Spring properties) and thread-safety concerns.
</details>

<details>
<summary>2. How does <code>@KafkaListener</code> manage concurrency?</summary>
By default, it spawns a single <code>MessageListenerContainer</code> thread. You can configure <code>concurrency</code> to spin up multiple threads to consume from multiple partitions simultaneously within the same application instance.
</details>

<details>
<summary>3. Why is <code>ErrorHandlingDeserializer</code> essential when dealing with JSON payloads?</summary>
If standard <code>JsonDeserializer</code> encounters malformed JSON, it throws an exception. Because the offset is never committed for that message, the consumer will fetch the exact same bad message in the next poll, creating a "poison pill" infinite loop. <code>ErrorHandlingDeserializer</code> catches the error, allowing the consumer to skip it and continue.
</details>
