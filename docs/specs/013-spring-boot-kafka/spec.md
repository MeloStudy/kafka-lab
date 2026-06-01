# Spec: LAB-013: Spring Boot & Kafka
**Status**: `[DRAFT]`

## 1. Pedagogical Objectives
- Understand how Spring abstracts Kafka operations via `KafkaTemplate` and `@KafkaListener`.
- Learn how to configure Spring Kafka producer and consumer properties via `application.yml`.
- Implement automatic JSON serialization/deserialization using Spring's `JsonMessageConverter` and Jackson.
- Understand the integration testing approach using `@SpringBootTest` and Testcontainers.

## 2. Core Concepts (CONCEPT.md)
- **Spring Kafka Architecture**: How `ConcurrentKafkaListenerContainerFactory` works behind the scenes.
- **KafkaTemplate**: The high-level abstraction for producing messages (similar to `JdbcTemplate`).
- **@KafkaListener**: Declarative consumer creation, threading models, and concurrency.
- **Message Conversion**: How payloads are transparently converted from POJOs to bytes and vice-versa using `JsonSerializer` / `JsonDeserializer`.

## 3. Infrastructure & Tooling
- Infrastructure Profile: Embedded Testcontainers for testing.
- Framework: Spring Boot 3.x, Spring Kafka.

## 4. Practical Implementation (README.md)
- Step 1: Bootstrap a Spring Boot project.
- Step 2: Configure `application.yml` for Kafka.
- Step 3: Implement a `PaymentProducer` using `KafkaTemplate`.
- Step 4: Implement a `PaymentConsumer` using `@KafkaListener` that logs received payments.
- Step 5: Test the flow using an integration test.

## 5. TDD & Technical Verification
- Test 1: **Spring Boot Integration Test**: Use `@SpringBootTest` alongside Testcontainers to spin up a real Kafka container.
- Verification: Send a Payment via the producer bean, and verify via a `CountDownLatch` or Mock that the listener received it.
- **Standards Check**: Use `@Slf4j`, package-private tests, chained assertions.

## 6. Resilience & Delivery Semantics
- **Resilience**: Introduction to the `ErrorHandlingDeserializer`. What happens when a consumer receives an invalid JSON payload? Explain how Spring Kafka handles deserialization exceptions to prevent poisonous messages from infinitely blocking the partition.

## 7. Self-Assessment Questions
1. What is the role of `KafkaTemplate` compared to the native `KafkaProducer`?
2. How does `@KafkaListener` manage concurrency, and how can you scale it to consume from multiple partitions?
3. Why is `ErrorHandlingDeserializer` essential when dealing with JSON payloads in Spring Kafka?
