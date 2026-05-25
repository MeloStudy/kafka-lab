# Spec: LAB-000: The Event-Driven Landscape & Kafka Fundamentals
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Understand the core differences between Request-Driven (REST) and Event-Driven Architectures (EDA).
- Distinguish between traditional Message Queues (RabbitMQ) and Event Streaming Platforms (Kafka).
- Understand Kafka's value proposition in the Enterprise: Scalability, Durability, Replayability.
- Develop architectural judgment to know exactly **when NOT to use Kafka**.

## 2. Core Concepts (CONCEPT.md)
*Theoretical concepts to be thoroughly explained:*
- **Event-Driven Architecture (EDA)**: Loose coupling, asynchronous communication, eventual consistency.
- **Messaging vs Streaming**: Push vs Pull models, transient queues vs durable append-only logs.
- **Kafka vs Alternatives**:
  - RabbitMQ/ActiveMQ (Smart broker, dumb consumer, complex routing).
  - AWS SNS + SQS (Cloud-native pub/sub & queueing, serverless, but ephemeral).
  - Redis Streams (In-memory, lightweight).
  - Kafka (Dumb broker, smart consumer, massive throughput, distributed log).
- **When NOT to use Kafka**: Small scale systems, strict request-response flows, lack of operational expertise to maintain a cluster.

## 3. Infrastructure & Tooling
*Define the infrastructure needed and CLI tools that should be demonstrated.*
- Infrastructure Profile: N/A (100% Theoretical/Architectural Lab).
- CLI Commands: N/A.

## 4. Practical Implementation (README.md)
*Describe the hands-on part.*
Since there is no code, the practical component is an **Architecture Decision Record (ADR) Simulation**.
- Scenario 1: A financial institution needing to route messages based on complex regex headers (Should they use RabbitMQ or Kafka?).
- Scenario 2: An e-commerce site where Order Creation must trigger inventory, billing, and notification services concurrently, and data must be replayable for audits (Should they use RabbitMQ or Kafka?).
- Scenario 3: A real-time chat application with strict latency requirements (microseconds) and ephemeral data.

## 5. TDD & Technical Verification
- TDD Exemption: No Java or Infrastructure involved in this lab. Technical verification is achieved through the Self-Assessment questions.

## 6. Resilience & Delivery Semantics
*Conceptual discussion only.*
- How Kafka achieves resilience (Replication, Partitions) vs how traditional queues do it.
- Why Delivery Semantics are a design decision and not a default guarantee.

## 7. Self-Assessment Questions
1. Why is Kafka considered a "distributed log" rather than a "message queue"?
2. What are 3 scenarios where choosing Kafka would be an architectural mistake?
3. How does the "Pull" model in Kafka differ from the "Push" model in traditional message brokers regarding backpressure?
