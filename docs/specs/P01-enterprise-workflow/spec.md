# Spec: P01-Enterprise-Workflow
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Bridge the gap between Synchronous HTTP requests and Asynchronous Event-Driven Architectures.
- Implement a fully non-blocking Reactive REST API using Spring WebFlux.
- Produce and Consume Kafka events reactively using Reactor Kafka / core Java clients.
- Validate the architecture using Testcontainers in a TDD workflow.
- Ensure the project is fully standalone, with its own parent-less `pom.xml` to simulate a real-world repository.

## 2. Core Concepts (CONCEPT.md)
- **Reactive Streams & Backpressure**: How Project Reactor integrates with Kafka's poll loop.
- **Bridging REST to Kafka**: Strategies for handling HTTP responses when the actual processing is asynchronous (e.g., returning HTTP 202 Accepted).
- **Eventual Consistency**: Understanding the delay between event publication and consumption.

## 3. Infrastructure & Tooling
- Infrastructure Profile: Basic KRaft (Single Broker) via Docker Compose.
- Tooling: Spring Boot 3.x, Spring WebFlux, Reactor Kafka, Testcontainers (Kafka).

## 4. Practical Implementation (README.md)
- **Domain**: Order Processing Flow.
- **Location**: `projects/P01-enterprise-workflow`
- **Step 1**: Expose `POST /api/orders` which receives an order payload.
- **Step 2**: The endpoint non-blockingly publishes an `OrderCreatedEvent` to an `orders.inbound` topic and immediately returns `202 Accepted`.
- **Step 3**: A Reactor Kafka Consumer listens to `orders.inbound`, simulates processing (e.g., Payment Validation), and emits an `OrderConfirmedEvent` to `orders.confirmed`.

## 5. TDD & Technical Verification
- **Test 1**: Integration test using Testcontainers. Send an HTTP POST to `/api/orders`, verify that an HTTP 202 is received.
- **Test 2**: Consumer Test using `EmbeddedKafka` or `Testcontainers`. Verify that publishing to `orders.inbound` triggers the consumer logic and produces a message in `orders.confirmed`.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: At-Least-Once delivery.
- **Resilience**: Demonstrate handling a failure during processing. If the "payment validation" fails, emit to an `orders.failed` Dead Letter Topic.
