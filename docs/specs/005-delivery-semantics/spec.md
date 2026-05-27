# Spec: LAB-005: Delivery Semantics & Transactions
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Understand the differences between At-most-once, At-least-once, and Exactly-once delivery semantics.
- Master the Kafka Transactional API for Consume-Process-Produce loops.
- Understand the role of the Transaction Coordinator and Two-Phase Commit (2PC) in Kafka.
- Implement consumer read isolation (`read_committed`).

## 2. Core Concepts (CONCEPT.md)
- **Delivery Semantics**: At-most-once, At-least-once, Exactly-once (EOS).
- **Idempotence**: `enable.idempotence=true`, sequence numbers, and producer IDs (PID).
- **Transactions**: `transactional.id`, Transaction Coordinator, 2PC mechanics.
- **Zombie Fencing**: Producer Epochs and handling `ProducerFencedException`.
- **Consumer Isolation**: `isolation.level=read_committed` vs `read_uncommitted`, Control Messages (Markers).

## 3. Infrastructure & Tooling
- **Infrastructure Profile**: Basic KRaft (`docs/templates/infra/docker-compose-basic.yml`).
- **CLI Commands**: 
  - `kafka-console-consumer.sh` with `--isolation-level read_committed` to observe transactional markers.

## 4. Practical Implementation (README.md)
- **Step 1**: Scaffold a Spring Boot / Java application with Kafka Clients.
- **Step 2**: Configure a transactional Producer with a static `transactional.id`.
- **Step 3**: Implement a Consume-Process-Produce pipeline:
  - `initTransactions()`
  - `beginTransaction()`
  - `sendOffsetsToTransaction()`
  - `commitTransaction()` / `abortTransaction()`
- **Step 4**: Configure a Consumer with `isolation.level=read_committed` to read the processed output.
- **Step 5**: Simulate an error mid-transaction to demonstrate `abortTransaction()` and verify the consumer ignores the aborted messages.

## 5. TDD & Technical Verification
- **Test 1**: Verify Exactly-Once Semantics using `TopologyTestDriver` or `Testcontainers` by injecting a failure mid-process. Assert that aborted messages are not readable by a `read_committed` consumer.
- **Test 2**: Verify Zombie Fencing. Instantiate two producers with the same `transactional.id`. Assert that the first producer throws a `ProducerFencedException` when attempting to commit.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: Exactly-Once (EOS).
- **Resilience**: The lab demonstrates handling zombie producers (network partitions causing split-brain producers) and transactional rollbacks on processing errors.
- **Backpressure/Polling**: Discuss how long-running transactions must not exceed `max.poll.interval.ms` to avoid consumer group rebalances while a transaction is open.

## 7. Self-Assessment Questions
1. How does the Transaction Coordinator ensure atomic commits across multiple partitions?
2. What happens if a consumer reads with `read_uncommitted` while a transaction is aborted?
3. How does `transactional.id` prevent the "zombie producer" problem?
