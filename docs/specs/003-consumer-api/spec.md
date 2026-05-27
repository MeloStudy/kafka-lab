# Spec: LAB-003: The Consumer API
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Master the `poll()` loop mechanics and understand why it is fundamentally different from a push-based webhook.
- Understand Consumer Groups and horizontal scaling (Partitions = Max Consumers).
- Demystify the internal mechanics of Consumer Rebalances (Eager vs. Cooperative).
- Learn the difference between Auto Commits and Manual Commits.

## 2. Core Concepts (CONCEPT.md)
*Theoretical concepts to be thoroughly explained:*
- **Consumer Groups & Offsets**: `group.id`, `__consumer_offsets` topic.
- **The `poll()` Loop Mechanics**: The background heartbeat thread vs the foreground processing thread.
- **Critical Configurations**: `max.poll.interval.ms`, `session.timeout.ms`, `max.poll.records`.
- **Rebalance Protocol**: Eager (Stop-the-world) vs Cooperative Sticky Assignor.

## 3. Infrastructure & Tooling
- **Infrastructure Profile**: Option A (Basic Apache KRaft Broker + Kafka-UI) using `docker-compose-basic.yml`.
- **Java Client**: `kafka-clients` library (Vanilla Java).

## 4. Practical Implementation (README.md)
*Describe the hands-on part.*
- **Step 1: Provisioning**: Start the cluster. Create a topic `lab003.events` with 3 partitions.
- **Step 2: Dummy Producer**: A simple Java application or script to continuously publish events to the topic.
- **Step 3: Consumer App**: A vanilla Java consumer that polls and prints messages.
- **Step 4: Horizontal Scaling**: Start 2 more instances of the consumer. Observe how partitions are assigned to them.
- **Step 5: Triggering a Rebalance**: Simulate a slow consumer by adding a `Thread.sleep` that exceeds `max.poll.interval.ms`. Observe the broker kicking the consumer out of the group and reassigning its partition.

## 5. TDD & Technical Verification
- **TDD Exemption**: Approved. Formal TDD with Testcontainers is explicitly introduced in LAB-004. Verification for this lab will be done visually via console logs and Kafka-UI observing the Consumer Group states and partition reassignments.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: Demonstrate **At-Least-Once** processing (the default). Show how failing to process a message but committing the offset leads to data loss (simulating At-Most-Once).
- **Flow Control**: Demonstrate backpressure implicitly. If processing takes too long, Kafka pauses the consumer via the `max.poll.interval.ms` limit.

## 7. Self-Assessment Questions
1. What happens if you have 3 partitions and you start 4 consumers with the same `group.id`?
2. Why does the Consumer have a separate heartbeat thread?
3. What is the danger of setting `enable.auto.commit=true` in a system that writes to a database?
