# Kafka Lab Constitution

## Core Principles

### I. Event-Driven First
The laboratory is built on the foundation of Event-Driven Architectures. Every lab MUST prioritize thinking in terms of immutable events rather than mutable state or synchronous REST calls.

### II. CLI & Tooling First
Before building abstractions with Java clients, interactions MUST be verified using native CLI tools (`kafka-console-producer.sh`, `kafka-topics.sh`) or a visual UI (like Kafka-UI). Building intuition on the raw platform is mandatory.

### III. The Java-First Path: Native Clients & Spring Kafka
To ensure maximum engineering rigor, the laboratory focuses exclusively on the **Java Ecosystem**. We start with native `kafka-clients` to understand the raw API, and progress to **Spring Kafka** and **Reactor Kafka** for enterprise integration.

### IV. Reproducible & Infrastructure-Aware (Progressive)
Kafka requires specific infrastructure. To prevent overwhelming cognitive load, the infrastructure MUST be progressive. 
- Early labs MUST use isolated, lightweight **Bitnami KRaft** containers per lab.
- Advanced labs MUST utilize **Testcontainers** for seamless integration testing and service connections.

### V. Educational Clarity & Theoretical Foundation
Theoretical depth is MANDATORY. Documenting APIs is not enough; each lab must explain the internal mechanics (e.g., how the Consumer Group protocol works, how partitions are assigned, how the poll loop functions). Theory MUST reside in `CONCEPT.md`, while practice stays in `README.md`.

### VI. Resilience by Design (Delivery Semantics)
Distributed systems fail. Every advanced lab MUST include scenarios where failures are simulated (network partitions, broker crashes) and handled using correct Delivery Semantics (At-Least-Once, Exactly-Once).

### VII. Modern Technology Assimilation & AI
As the software landscape evolves, the laboratory MUST evaluate how Event-Driven architectures integrate with AI patterns. This includes using Kafka as the backbone for Real-Time Context, feeding Vector Databases, and powering Streaming RAG pipelines.

### VIII. Enterprise Observability
Without observability, reactive Kafka systems are effectively unoperable. Logs must describe the event flow (using Trace IDs and Correlation IDs), and Metrics must provide continuous visibility into system health (Consumer Lag, Throughput, Error Rates).

## Lab Design Standards

- **Spec Planning**: Mandatory `spec.md`, `plan.md`, and `tasks.md` before coding. These artifacts MUST reside in `docs/specs/{lab-slug}/`.
- **Agentic Lifecycle**: Labs MUST strictly transition through states: `[PLANNED] -> [DRAFT] -> [READY] -> [DONE]`. No lab can be coded unless its state is explicitly `[READY]`.
- **TDD Mandatory**: All `[READY]` labs must be implemented by writing failing tests BEFORE the actual Java/Kafka implementation. Tests MUST use `TopologyTestDriver`, `EmbeddedKafka`, or `Testcontainers` depending on the layer being tested.
- **Naming Convention**: `XXX-slug-name` (e.g., `001-architecture-core`).
- **Interactive Self-Assessment**: Every laboratory README MUST include a "Self-Assessment" or "Knowledge Check" section using collapsible `<details>` blocks to provide immediate pedagogical feedback.
- **Backpressure Scenarios**: Labs involving data streams MUST explain how backpressure is handled, particularly the `poll()` mechanism in Kafka consumers.
