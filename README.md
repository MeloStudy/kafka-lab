# Kafka Masterclass Laboratory 🚀

Welcome to the **Kafka Masterclass Laboratory**. This repository contains a series of hands-on laboratories designed to master Apache Kafka, from foundational event-driven concepts to advanced stream processing and enterprise integration.

## 🧠 Why this curriculum? (The Pedagogical Path)
Learning Kafka is more than just learning an API; it requires a shift to **Event-Driven Architecture**. We approach this in progressive levels using **Java 21, Spring Boot, and Confluent Platform**:

1. **The Mindset**: Foundational concepts of events, pub/sub, logs, and partitions.
2. **The Core**: Mastering the Producer/Consumer APIs, delivery semantics, and schema management.
3. **The Ecosystem**: Connecting systems with Kafka Connect and building real-time pipelines with Kafka Streams.
4. **The Enterprise**: Integrating with Spring Boot, Reactor Kafka, Observability, and Testcontainers.
5. **The Horizon**: Event Sourcing, CQRS, and Multi-DC patterns.

## 🎯 Learning Path Overview
For a detailed breakdown of all modules, learning objectives, and the certification path, see the [Full Syllabus](docs/syllabus.md).

- **Level 1: The Event-Driven Mindset & Foundations**
- **Level 2: Advanced Core & Resiliency**
- **Level 3: The Kafka Ecosystem (Connect & Streams)**
- **Level 4: Enterprise & Reactive Integration**
- **Level 5: Architecture & The Horizon**

*Note: The curriculum includes progressive mini-projects and a final Capstone Project to consolidate knowledge.*

## 🛠️ Prerequisites & Tech Stack
To successfully run these laboratories, you need the following environment:

- **Java 21 (LTS)**
- **Maven**
- **Docker & Docker Compose** (for running the local Confluent Platform cluster)

### Local Environment Setup
We use a **Progressive Environment** approach for infrastructure.
- In early labs (e.g., Level 1 & 2), you will find a minimal `docker-compose.yml` (using Bitnami Kafka) inside the specific lab directory to help you focus on the basics without overwhelming complexity.
- In advanced labs (Level 4+), we will utilize **Spring Boot Testcontainers (Service Connections)** where the Java application automatically provisions ephemeral Kafka containers, allowing you to focus 100% on code.

To start a local environment for a specific lab, navigate to its directory and run:
```bash
docker-compose up -d
```

## 🚀 How to use this repository
1. Go to `docs/syllabus.md` and follow the labs in order.
2. Each lab is located inside the `labs/` directory as an independent Maven module.
3. Mini-projects and the final Capstone are located in the `projects/` directory.

### 🤖 AI Agent Workflow Guide
This repository is designed to be built and maintained in collaboration with an AI Agent. For a detailed explanation of the commands used to generate and audit the labs, please see the **[Agent Workflow Guide](docs/agent-workflows.md)**.

Enjoy your journey into Event-Driven Systems!
