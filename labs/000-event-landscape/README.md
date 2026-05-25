# LAB-000: The Event-Driven Landscape & Kafka Fundamentals

Welcome to the first laboratory. Before we dive into the technical details of clusters, partitions, and Java code, we must develop strong architectural judgment. 

Please ensure you have read the theory in [`CONCEPT.md`](CONCEPT.md) before proceeding.

## 🏗️ Architecture Decision Record (ADR) Simulation

In the real world, you don't just "use Kafka". You evaluate requirements and make trade-offs. Read the following scenarios and decide whether Kafka is the right tool for the job.

### Scenario 1: The E-Commerce Checkout
**Requirements**: When a user clicks "Buy", the system must immediately charge their credit card, reserve inventory, and confirm the order on the screen within 500ms. If the credit card fails, the order must not be created.
- **Your Task**: Should this be an Event-Driven flow using Kafka, or a synchronous REST/gRPC flow?
- **Decision**: <details><summary>Click to reveal</summary>
  **REST / Synchronous RPC**. This is a strict transactional flow where the user is waiting for an immediate, definitive response (Success/Fail). Kafka is asynchronous. If you place the "Charge Card" request in Kafka, the user's browser would have to poll endlessly waiting for a result. Use REST or gRPC for immediate Request-Response.
  </details>

### Scenario 2: The Logistics Tracking System
**Requirements**: A fleet of 10,000 delivery trucks sends GPS coordinates every 5 seconds. The data needs to be used by a real-time dashboard, a machine-learning model that predicts traffic, and a daily batch job that calculates driver pay. All 3 systems need to read the exact same data, but at their own pace.
- **Your Task**: Should you use RabbitMQ, AWS SQS, or Apache Kafka?
- **Decision**: <details><summary>Click to reveal</summary>
  **Apache Kafka**. This is the perfect use case for a durable Event Stream. RabbitMQ and SQS delete messages once they are consumed (or require complex fan-out architectures where messages are duplicated). Kafka stores the GPS events in an append-only log, allowing the real-time dashboard to read them immediately, while the batch job reads them hours later from the same topic.
  </details>

### Scenario 3: The Background Email Processor
**Requirements**: You run a SaaS application. When users sign up, you need to send them a welcome email. It doesn't matter if the email arrives instantly or 2 minutes later. You want to spin up 5 worker nodes to process these emails concurrently.
- **Your Task**: Is Kafka the best choice here?
- **Decision**: <details><summary>Click to reveal</summary>
  **AWS SQS or RabbitMQ**. While Kafka *can* do this, it's overkill. This is a classic "Work Queue" pattern. You just need a queue where competing workers grab a task, process it, and acknowledge it. In Kafka, you are limited by the number of partitions (you can't have 100 workers processing a 10-partition topic concurrently). SQS or RabbitMQ allow infinite competing consumers on a single queue.
  </details>

### Scenario 4: Legacy Database Synchronization (CDC)
**Requirements**: Your company has a massive, 15-year-old Oracle database that serves as the single source of truth. You are building a new microservice that provides lightning-fast search capabilities using Elasticsearch. Every time a row is inserted, updated, or deleted in Oracle, the change must be reliably mirrored to Elasticsearch. You also plan to add a caching layer (Redis) in the future that will need the exact same stream of updates.
- **Your Task**: Should you write a batch job, use RabbitMQ, or use Apache Kafka?
- **Decision**: <details><summary>Click to reveal</summary>
  **Apache Kafka**. This is the textbook scenario for Change Data Capture (CDC). Kafka acts as the immutable, strictly-ordered backbone. Tools like Debezium can read the Oracle transaction log and stream changes directly into Kafka. Both Elasticsearch and Redis can consume this exact same stream independently (Fan-out) and Kafka's durability ensures no data is lost if the search service crashes for a few hours.
  </details>

---

## 📝 Self-Assessment

To certify that you have completed this laboratory, you must be able to answer the following questions clearly.

<details>
<summary><b>1. Why is Kafka considered a "distributed log" rather than a "message queue"?</b></summary>
<br>
A traditional queue deletes a message once a consumer acknowledges it. Kafka acts like a file appending data sequentially to a disk. Messages are retained for a configurable period, meaning multiple independent consumers can read the exact same data again and again by tracking their own "offset" (position) in the log.
</details>

<details>
<summary><b>2. How does the "Pull" model in Kafka differ from the "Push" model in traditional message brokers regarding backpressure?</b></summary>
<br>
In a Push model, the broker dictates the speed. If a consumer is slow, the broker might overwhelm it (requiring complex pre-fetch limits). In Kafka's Pull model, consumers explicitly ask for batches of messages only when they are ready. This naturally provides backpressure; a slow consumer simply falls behind (increasing "consumer lag") but does not crash due to an overwhelming flood of pushed messages.
</details>

<details>
<summary><b>3. What happens if you try to use Kafka for a strict low-latency Request-Reply architecture?</b></summary>
<br>
You will create accidental complexity. You would have to publish to a request topic, and the client would have to subscribe to a response topic, correlating IDs to figure out which response belongs to which request. This is slow, hard to debug, and fragile compared to a simple HTTP request.
</details>

---
> [!TIP]
> If you understood the concepts and passed the simulation, you are ready for the technical deep dive. Proceed to **LAB-001** to spin up your first Kafka cluster!
