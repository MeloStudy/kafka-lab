# Spec: LAB-011: Kafka Streams API - Stateless Processing
**Status**: `[DRAFT]`

## 1. Pedagogical Objectives
- Understand the Kafka Streams DSL and the concept of a `Topology`.
- Implement stateless transformations: `map`, `filter`, `flatMap`, `branch`.
- Understand the execution model: Stream Threads, Tasks, and Partitions.

## 2. Core Concepts (CONCEPT.md)
*List the theoretical concepts that need to be explained.*
- **Kafka Streams vs Consumer/Producer API**: Why use a stream processing framework?
- **Stream Topology**: Source nodes, Processor nodes, and Sink nodes.
- **KStream**: An abstraction of a record stream (insert-only).
- **Stateless Operations**: Transformations that don't require context from previous events (Map, Filter, Peek).
- **Threading Model**: How `num.stream.threads` maps to Kafka partitions and Tasks.

## 3. Infrastructure & Tooling
*Define the infrastructure needed (e.g., `docker-compose-basic.yml`) and CLI tools that should be demonstrated.*
- Infrastructure Profile: Basic KRaft (Broker only).
- CLI Commands: `kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh` to verify inputs/outputs.
- Java: `kafka-streams` library natively.

## 4. Practical Implementation (README.md)
*Describe the code or hands-on part.*
- Step 1: Create an input topic `raw-transactions` and an output topic `high-value-transactions`.
- Step 2: Implement a `KafkaStreams` application using the DSL.
- Step 3: Use `filter` to only keep transactions > $1000.
- Step 4: Use `mapValues` to mask sensitive data in the transaction payload.
- Step 5: Start the application and produce sample data via CLI; consume from output to verify.

## 5. TDD & Technical Verification
*Define the exact JUnit/Testcontainers tests that MUST be written BEFORE the implementation.*
- Test 1: **TopologyTestDriver**: Write unit tests without a real broker to verify the `Topology`. `TopologyTestDriver` is the standard for fast, deterministic Streams testing.
- Test 2: Verify `filter` logic drops small transactions.
- Test 3: Verify `mapValues` logic correctly obfuscates data.

## 6. Resilience & Delivery Semantics
*Define the exact Delivery Semantics (At-Most-Once, At-Least-Once, Exactly-Once) used in this lab.*
- **Delivery Semantics**: At-Least-Once (default in Streams).
*How does this lab demonstrate handling failures or flow control (Backpressure/Polling)?*
- **Failure Handling**: What happens if a Stream Thread dies? Explain partition reassignment and stream task migration.

## 7. Self-Assessment Questions
1. Why is `TopologyTestDriver` preferred over `EmbeddedKafka` or `Testcontainers` for unit testing Streams topologies?
2. If your input topic has 6 partitions, and you configure `num.stream.threads=2` across 2 application instances (4 threads total), how are tasks distributed?
3. What is the difference between `map` and `mapValues`, and why is `mapValues` preferred when possible?
