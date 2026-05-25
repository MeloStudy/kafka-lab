# Spec: LAB-001: Architecture Core & Infrastructure
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Understand the physical and logical architecture of a Kafka cluster.
- Learn how to deploy a modern, Zookeeper-less Kafka instance using **KRaft**.
- Gain intuition on how topics and partitions work by interacting directly with the cluster.
- Learn to manage Kafka using native CLI tools rather than relying immediately on a programming language.

## 2. Core Concepts (CONCEPT.md)
*Theoretical concepts to be thoroughly explained:*
- **The Cluster**: Brokers and their roles.
- **Topics & Partitions**: Why we partition data (horizontal scaling and parallelism).
- **Replication Factor & ISR**: How Kafka achieves High Availability. What an In-Sync Replica (ISR) is and why it matters for durability.
- **KRaft Protocol**: The shift from Zookeeper to KRaft (KIP-500) and how the active controller is elected internally.

## 3. Infrastructure & Tooling
*Define the infrastructure needed and CLI tools that should be demonstrated.*
- **Infrastructure Profile**: Option A (Basic KRaft Broker + Kafka-UI). We will use `docker-compose-basic.yml` which deploys a single Bitnami Kafka node and Provectus Kafka-UI.
- **CLI Commands**:
  - `kafka-topics.sh`: `--create`, `--list`, `--describe`, `--alter`.
  - `kafka-console-producer.sh`: Producing plain text messages.
  - `kafka-console-consumer.sh`: Consuming messages, exploring `--from-beginning`.

## 4. Practical Implementation (README.md)
*Describe the hands-on part.*
- **Step 1: Provisioning**: Start the cluster using Docker Compose. Open Kafka-UI (`http://localhost:8080`) to verify the broker is alive.
- **Step 2: CLI Access**: Exec into the Kafka container (`docker exec -it kafka bash`) to run CLI tools.
- **Step 3: Managing Topics**: Create a topic `lab001.events` with 3 partitions and replication factor 1. Describe the topic to view the Leader and ISR list.
- **Step 4: Publishing & Consuming**: Use the console producer to send 5 messages. Use the console consumer to read them. Stop the consumer, send 5 more messages, and show how the consumer only reads the new ones unless `--from-beginning` is used.

## 5. TDD & Technical Verification
- **TDD Exemption**: Approved. As this lab is purely infrastructure and CLI-based, no JUnit/Java code will be written. Verification is achieved by successfully running the commands and visualizing the data in Kafka-UI.

## 6. Resilience & Delivery Semantics
*Conceptual discussion and practical limits in this lab.*
- Because we are running a single broker, the replication factor is strictly limited to 1.
- Real-world resilience requires a minimum of 3 brokers (to allow RF=3, `min.insync.replicas=2`). This will be simulated conceptually.

## 7. Self-Assessment Questions
1. Why is KRaft considered superior to Zookeeper for modern Kafka deployments?
2. If you create a topic with 5 partitions, but you only have 3 brokers, is this allowed? Why?
3. What does it mean if a replica falls out of the ISR (In-Sync Replicas) list?
