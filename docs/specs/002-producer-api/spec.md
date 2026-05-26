# Spec: LAB-002: The Producer API
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Master the internal mechanics of the Kafka Producer.
- Understand the trade-offs between throughput, latency, and durability.
- Learn how to achieve strict ordering using Message Keys and the Idempotent Producer.
- Understand how batching and compression affect performance.

## 2. Core Concepts (CONCEPT.md)
- **Acks (0, 1, all)**: Durability guarantees and potential data loss scenarios.
- **Retries & Delivery Timeout**: How producers recover from transient errors without blocking indefinitely (`delivery.timeout.ms`).
- **Batching & Linger (`batch.size`, `linger.ms`)**: Optimizing network requests for throughput.
- **Compression (`snappy`, `zstd`)**: Reducing payload size and network bandwidth.
- **Partitioner**: Default partitioning strategy (Sticky Partitioner) vs custom partitioning.
- **The Idempotent Producer (`enable.idempotence=true`)**: Preventing duplicate messages on retries.
- **Message Keys**: Guaranteeing order for a specific entity.
- **Headers**: Passing metadata like Correlation/Trace IDs without modifying the payload.

## 3. Infrastructure & Tooling
- **Infrastructure Profile**: Option A: Basic KRaft (`docs/templates/infra/docker-compose-basic.yml`)
- **CLI Commands**: 
  - `kafka-topics.sh` (create topics to test partitioning)
  - `kafka-console-consumer.sh` (with property `--property print.headers=true` to observe headers)

## 4. Practical Implementation (README.md)
- **Step 1**: Scaffold a Java Maven project with `kafka-clients` and Testcontainers.
- **Step 2**: Implement a basic Producer (focusing on Acks).
- **Step 3**: Implement a High-Throughput Producer (tweaking `batch.size`, `linger.ms`, `compression.type`).
- **Step 4**: Implement an Idempotent Producer with message keys and headers.
- **Step 5**: Simulate failure scenarios (e.g., stopping a broker) and observe retries.

## 5. TDD & Technical Verification
- **Test 1**: Verify messages with the same key end up in the same partition using `KafkaProducer` and Testcontainers.
- **Test 2**: Verify Idempotent producer properties are correctly configured and duplicates are avoided during retries.
- **Test 3**: Verify Headers (Correlation IDs) are correctly attached to the `ProducerRecord` and retrievable.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: The lab will demonstrate the shift from At-Least-Once (default historically, though idempotence is default in newer clients) to Exactly-Once (single-partition) using the Idempotent Producer.
- **Resilience Scenarios**: 
  - Network jitter/transient broker failure (demonstrate Retries).
  - Backpressure (handling `BufferExhaustedException` when `buffer.memory` is full).

## 7. Self-Assessment Questions
1. What happens if `linger.ms` is set to 0? How does it affect throughput and latency?
2. Why is `acks=all` recommended (and enforced) when `enable.idempotence=true`?
3. How do message keys guarantee ordering, and what happens if the number of topic partitions changes dynamically?
