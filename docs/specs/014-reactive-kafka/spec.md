# Spec: LAB-014: Reactive Kafka (WebFlux Integration)
**Status**: `[DRAFT]`

## 1. Pedagogical Objectives
- Understand the integration between Project Reactor and Kafka.
- Learn how to bridge asynchronous REST APIs (Spring WebFlux) to Kafka publishers (`reactor-kafka`).
- Master Reactive Pipeline Consumption using `KafkaReceiver`.
- Implement non-blocking backpressure mechanisms and avoid thread starvation.

## 2. Core Concepts (CONCEPT.md)
- Concept 1: **Project Reactor & Reactive Streams**: Flux, Mono, and non-blocking backpressure.
- Concept 2: **reactor-kafka Architecture**: `KafkaSender` and `KafkaReceiver` abstractions vs standard `KafkaTemplate` and `@KafkaListener`.
- Concept 3: **Bridging REST to Kafka**: Handling inbound HTTP requests reactively and publishing events to Kafka without blocking the Netty event loop.
- Concept 4: **Reactive Consumption & Backpressure**: How `KafkaReceiver` translates Kafka's pull-based `poll()` model into a push-based Reactive Stream with backpressure, managing offsets manually and safely.

## 3. Infrastructure & Tooling
- Infrastructure Profile: Option C: Testcontainers (Service Connections). No `docker-compose.yml` needed for local development, managed automatically by Spring Boot in Java.
- CLI Commands: None strictly required, though `kafka-consumer-groups.sh` is useful to observe offset commits during reactive consumption.

## 4. Practical Implementation (README.md)
- Step 1: Create a Spring Boot application with WebFlux and `reactor-kafka` dependencies.
- Step 2: Implement a Reactive REST Endpoint (`RestController`) that receives a payload, transforms it, and uses `KafkaSender` to publish the event to a `transactions` topic.
- Step 3: Implement a `KafkaReceiver` pipeline that consumes the `transactions` topic.
- Step 4: Simulate a slow downstream service (e.g., using `delayElements`) to demonstrate backpressure. Ensure that the `KafkaReceiver` pauses polling from Kafka.
- Step 5: Implement manual offset commitment using `ReceiverOffset.acknowledge()` within the reactive chain to guarantee at-least-once delivery.

## 5. TDD & Technical Verification
- Test 1: Write an `@WebFluxTest` to verify the REST endpoint behaves non-blocking and correctly calls the `KafkaSender`.
- Test 2: Write an Integration Test using `@Testcontainers` to produce a message to the `transactions` topic and verify the reactive consumer processes it and commits the offset.
- Test 3: Verify backpressure: send a burst of messages and assert that consumption rate matches the downstream delay without dropping messages or exhausting memory.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: **At-Least-Once**. We will manually acknowledge offsets (`ReceiverOffset.acknowledge()`) only after the reactive pipeline has successfully processed the event.
- **Resilience**: The lab will demonstrate how `onErrorResume` and `retry` operators can be used in the reactive pipeline to handle transient failures without crashing the application or losing messages.
- **Backpressure**: The core of the lab is demonstrating how Reactor's backpressure propagates up to the `KafkaReceiver`, which automatically pauses its internal `poll()` loop when the downstream subscriber cannot keep up, preventing `OutOfMemoryError`.

## 7. Self-Assessment Questions
1. How does `reactor-kafka` translate Reactor's backpressure signal into Kafka's `poll()` mechanics?
2. What happens if you block the thread within a `flatMap` step of the `KafkaReceiver` pipeline?
3. How do you ensure At-Least-Once delivery in a reactive pipeline, and why shouldn't you enable auto-commit?
