# Spec: LAB-010: Kafka Connect Basics
**Status**: `[DRAFT]`

## 1. Pedagogical Objectives
- Understand the role of Kafka Connect in the broader Kafka ecosystem.
- Differentiate between Source and Sink connectors.
- Understand the difference between Standalone and Distributed execution modes.
- Implement Change Data Capture (CDC) using Debezium.

## 2. Core Concepts (CONCEPT.md)
*List the theoretical concepts that need to be explained.*
- Concept 1: **Kafka Connect Architecture**: Workers, Tasks, Converters (Avro/JSON), and Transformers (SMTs).
- Concept 2: **Standalone vs Distributed Mode**: Fault tolerance, scalability, and REST API management.
- Concept 3: **Change Data Capture (CDC)**: How Debezium tails database transaction logs instead of polling.
- Concept 4: **Delivery Semantics in Connect**: How offsets are managed for Source (in Kafka) and Sink (in target system) connectors.

## 3. Infrastructure & Tooling
*Define the infrastructure needed (e.g., `docker-compose-basic.yml`) and CLI tools that should be demonstrated.*
- Infrastructure Profile: Kafka broker, Schema Registry, Kafka Connect worker (with Debezium plugin), PostgreSQL database (for CDC).
- CLI Commands/API: 
  - `curl` commands to interact with the Kafka Connect REST API (`GET /connectors`, `POST /connectors`).
  - Kafka UI for inspecting topics and schemas.

## 4. Practical Implementation (README.md)
*Describe the code or hands-on part.*
- Step 1: Start the Docker Compose infrastructure (Kafka, Connect, Postgres).
- Step 2: Initialize the PostgreSQL database with a `users` table and some records.
- Step 3: Register a Debezium PostgreSQL Source Connector via the Connect REST API.
- Step 4: Consume the CDC events from the generated Kafka topic using Kafka-UI.
- Step 5: Perform an UPDATE/DELETE in PostgreSQL and observe the corresponding CDC event (e.g., tombstone for DELETE).

## 5. TDD & Technical Verification
*Define the exact JUnit/Testcontainers tests that MUST be written BEFORE the implementation.*
- Test 1: **Testcontainers setup**: Spin up Kafka, Postgres, and a custom Kafka Connect container.
- Test 2: **CDC Verification**: Insert a record into the Postgres container, wait, and verify an event is published to the corresponding Kafka topic using `KafkaConsumer`.

## 6. Resilience & Delivery Semantics
*Define the exact Delivery Semantics (At-Most-Once, At-Least-Once, Exactly-Once) used in this lab.*
- **Delivery Semantics**: At-Least-Once (default for most source connectors). Explain how Debezium manages offsets to resume reading the WAL from where it left off.
*How does this lab demonstrate handling failures or flow control (Backpressure/Polling)?*
- We will simulate a Connect worker restart to demonstrate that no database changes are missed (CDC log tailing resilience).

## 7. Self-Assessment Questions
1. Why is tailing a transaction log (CDC) better than periodically polling a database table?
2. Where does Kafka Connect store its configuration and offset data in Distributed mode?
3. What is the role of a Converter in Kafka Connect?
