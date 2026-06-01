# Spec: LAB-012: Kafka Streams API - Stateful Processing
**Status**: `[DRAFT]`

## 1. Pedagogical Objectives
- Understand the difference between `KStream` (Event Stream) and `KTable` (Changelog Stream).
- Understand how Kafka Streams manages state using local RocksDB stores and Kafka changelog topics.
- Implement stateful aggregations (e.g., `count`, `reduce`, `aggregate`).
- Understand and implement Time Windowing (Tumbling).

## 2. Core Concepts (CONCEPT.md)
*List the theoretical concepts that need to be explained.*
- **KTable & GlobalKTable**: The duality of Streams and Tables.
- **State Stores (RocksDB)**: How local state is persisted on disk for performance.
- **Changelog Topics**: How Kafka provides fault tolerance for local state stores.
- **Aggregations & Grouping**: `groupBy` / `groupByKey` operations and their impact (Repartitioning).
- **Windowing**: Grouping events by time (Tumbling vs Hopping vs Session windows).

## 3. Infrastructure & Tooling
*Define the infrastructure needed (e.g., `docker-compose-basic.yml`) and CLI tools that should be demonstrated.*
- Infrastructure Profile: Basic KRaft (Broker only).
- CLI Commands: `kafka-topics.sh` to observe internal changelog and repartition topics created by stateful operations.
- Java: `kafka-streams` library natively.

## 4. Practical Implementation (README.md)
*Describe the code or hands-on part.*
- Step 1: Create an input topic `user-clicks`.
- Step 2: Implement a `KafkaStreams` application that computes the total clicks per user (`KTable`).
- Step 3: Implement a windowed aggregation computing clicks per user every 1 minute (Tumbling Window).
- Step 4: Run the application and produce events. Observe the aggregated output.
- Step 5: Observe the internal topics (`<app-id>-<store-name>-changelog`) created by Kafka Streams using the CLI.

## 5. TDD & Technical Verification
*Define the exact JUnit/Testcontainers tests that MUST be written BEFORE the implementation.*
- Test 1: **TopologyTestDriver**: Verify the global click aggregation.
- Test 2: **Windowed Aggregation Test**: Advance the test driver's wall-clock time (`advanceWallClockTime`) to verify window closures and emissions.

## 6. Resilience & Delivery Semantics
*Define the exact Delivery Semantics (At-Most-Once, At-Least-Once, Exactly-Once) used in this lab.*
- **Delivery Semantics**: Exactly-Once Semantics (EOS) intro (`processing.guarantee="exactly_once_v2"`).
*How does this lab demonstrate handling failures or flow control (Backpressure/Polling)?*
- **Failure Handling (State Restoration)**: If a Streams instance crashes, its local RocksDB state is lost. Explain how the replacement thread reconstructs the state store by replaying the changelog topic from Kafka before resuming processing.

## 7. Self-Assessment Questions
1. What is the fundamental difference between a `KStream` and a `KTable`?
2. Why does `groupByKey()` followed by an aggregation often create an internal "repartition" topic?
3. If the local disk fails and the RocksDB state store is corrupted, how does Kafka Streams recover?
