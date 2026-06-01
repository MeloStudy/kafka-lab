# Kafka Programming Lab Syllabus

## Level 1: The Event-Driven Mindset & Foundations
Focus: Shifting the mindset from REST/Synchronous to Event-Driven and mastering Kafka's core mechanics.

- [DONE] **LAB-000: The Event-Driven Landscape & Kafka Fundamentals**
  - Concepts: Message Queues vs Event Streams (RabbitMQ vs Kafka).
  - Deep Dive: Why Kafka? Use cases, architectural trade-offs, and **when NOT to use Kafka**.
  - Practical: Theoretical assessment and understanding the event backbone.
- [DONE] **LAB-001: Architecture Core & Infrastructure**
  - Concepts: Brokers, Topics, Partitions, Replicas, **ISR (In-Sync Replicas)**.
  - Deep Dive: **KRaft (KIP-500/833)** vs Legacy Zookeeper.
  - Practical: Dockerized (Apache KRaft), **Kafka-UI** for observability, and **CLI Tools** (`kafka-topics.sh` with create/alter/delete, `kafka-console-producer.sh`).
- [DONE] **LAB-002: The Producer API**
  - Concepts: Acks (0, 1, all), Retries, Batching (`linger.ms`, `batch.size`), Compression (`snappy`, `zstd`), Partitioner.
  - Deep Dive: **The Idempotent Producer** (`enable.idempotence=true`), **Message Keys** (Ordering guarantees), and **Headers** (Correlation/Trace IDs).
- [DONE] **LAB-003: The Consumer API**
  - Concepts: Consumer Groups, Offsets (Auto vs Manual), **Heartbeat Thread**, `poll()` loop mechanics, `max.poll.interval.ms`.
  - Deep Dive: **Rebalance Protocol** (Eager vs Cooperative Sticky Assignor).
- [DONE] **LAB-004: Testing Fundamentals (TDD)**
  - Concepts: `TopologyTestDriver`, `EmbeddedKafka`.
  - Deep Dive: **Testcontainers** for robust integration testing, establishing the TDD baseline for all future labs.
- [DONE] **P01: Enterprise Event-Driven Workflow**
  - Objective: Build a complete end-to-end flow (e.g., Order Processing, User Registration) that receives an HTTP request, publishes an event, and processes it using **Reactive Programming (Spring WebFlux & Project Reactor)**, core Java clients, and TDD.

## Level 2: Advanced Core & Resiliency
Focus: Deep dive into internal mechanics, failure recovery, and advanced control.

- [DONE] **LAB-005: Delivery Semantics & Transactions**
  - Concepts: At-most-once, At-least-once, **Exactly-once (EOS)**.
  - Deep Dive: Transaction Coordinator, `transactional.id`, 2PC (Two-Phase Commit) in Kafka.
- [DONE] **LAB-006: Advanced Consumer Patterns**
  - Concepts: Manual Offset Commits (Sync vs Async), Consumer Rebalance Listeners, Standby Consumers.
- [DONE] **LAB-007: Data Contracts & Schema Registry**
  - Concepts: Avro, Protobuf, Schema Evolution (Forward, Backward, Full Compatibility).
  - Deep Dive: Confluent Schema Registry internals and Data Governance principles.
- [PLANNED] **LAB-008: Advanced Topic Configurations & Storage**
  - Concepts: Log Compaction (Tombstone messages), Retention policies (Time vs Size).
  - Deep Dive: **Tiered Storage (KIP-405)** (Decoupling Compute from S3/Remote Storage).
- [DONE] **LAB-009: Advanced Error Handling & DLTs**
  - Concepts: Exception Classification (Fatal vs Transient), Dead Letter Topics (DLT).
  - Deep Dive: Non-blocking Retries (`@RetryableTopic`), `DefaultErrorHandler`.

## Level 3: The Kafka Ecosystem (Connect & Streams)
Focus: Utilizing the broader ecosystem for data integration and stateful processing.

- [DONE] **LAB-010: Kafka Connect Basics**
  - Concepts: Source/Sink Connectors, Standalone vs Distributed, Connect Workers.
  - Deep Dive: CDC (Change Data Capture) using Debezium.
- [DONE] **LAB-011: Kafka Streams API - Stateless Processing**
  - Concepts: KStream, Map, Filter, Branch, Topology, Stream Threads.
- [DRAFT] **LAB-012: Kafka Streams API - Stateful Processing**
  - Concepts: KTable, GlobalKTable, State Stores (RocksDB).
  - Deep Dive: Aggregations, Windowing (Tumbling, Hopping, Session), Joins (Stream-Stream, Stream-Table).
- [PLANNED] **MINI-PROJECT-2: Real-time Analytics Dashboard**
  - Objective: Build an end-to-end analytics pipeline using Kafka Streams, Connect (CDC), and expose the results via a **Reactive Server-Sent Events (SSE) API using Spring WebFlux**.

## Level 4: Enterprise & Reactive Integration
Focus: Integrating Kafka with modern Spring frameworks and deep observability.

- [PLANNED] **LAB-013: Spring Boot & Kafka**
  - Concepts: Spring Kafka Architecture, `@KafkaListener`, `KafkaTemplate`, Message Conversion.
- [DONE] **LAB-014: Reactive Kafka (WebFlux Integration)**
  - Concepts: Project Reactor integration, `reactor-kafka`, Receiver/Sender, **Bridging REST to Kafka**.
  - Deep Dive: **Reactive Pipeline Consumption**, Non-blocking backpressure, and avoiding thread starvation.
- [PLANNED] **LAB-015: Observability, Tracing & Chaos**
  - Concepts: Micrometer, JMX Metrics.
  - Deep Dive: Distributed Tracing (OpenTelemetry/Span IDs), Chaos Engineering (Toxiproxy for network partitions).

## Level 5: Architecture & The Horizon
Focus: System design, security, and next-generation AI integrations.

- [PLANNED] **LAB-016: Cluster Sizing & Capacity Planning**
  - Concepts: Sizing math for Brokers, Partitions, and Replication Factors based on SLA and Throughput.
  - Deep Dive: Hardware considerations (Disk, CPU, Network) and Partition limits.
- [PLANNED] **LAB-017: Security**
  - Concepts: SSL/TLS encryption, SASL/SCRAM authentication, ACLs (Access Control Lists).
- [PLANNED] **LAB-018: Multi-Datacenter & MirrorMaker 2**
  - Concepts: Active/Active, Active/Passive setups, Offset translation, MM2 Connectors.
- [PLANNED] **LAB-019: Event Sourcing & CQRS Patterns**
  - Concepts: Kafka as Source of Truth, Event Store vs State Store, Command vs Query separation.
- [PLANNED] **LAB-020: Kafka in the AI Era (Real-Time Context)**
  - Concepts: Streaming/Batch convergence, Real-time Context for Agents.
  - Deep Dive: Feeding RAG (Retrieval-Augmented Generation) pipelines and Vector Databases via Kafka.
- [PLANNED] **CAPSTONE PROJECT: Enterprise Event-Driven Microservices**
  - Objective: A fully-fledged architecture using Spring WebFlux, Reactor Kafka, Schema Registry, Testcontainers, and AI Context Streams.
