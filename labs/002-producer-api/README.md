# LAB-002: The Producer API

## Overview
This lab covers the internal mechanics of the Kafka Producer API, including Acks, Retries, Batching, Compression, Idempotence, Message Keys, and Headers.

> **Status**: `[DONE]`

## Infrastructure Setup

To begin, start the local KRaft cluster.
```bash
docker-compose up -d
```

### Infrastructure Dissection
The `docker-compose.yml` uses Kafka in KRaft mode (no Zookeeper).
- `KAFKA_PROCESS_ROLES=controller,broker`: This single container acts as both the data broker and the cluster controller.
- `KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092...`: We configure internal routing for Docker-to-Docker communication (29092) and external for your host machine (9092).
- `KAFKA_CONTROLLER_QUORUM_VOTERS=0@kafka:9093`: Defines the quorum. Since it's a single node, it votes for itself.

## Hands-On CLI Practice

Before writing Java code, let's test using the CLI.

### 1. Create a Topic
Create a topic with 3 partitions to test message keys.
```bash
docker exec -it <container_name_or_id> \
  kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic producer-lab \
  --partitions 3
```
**Command Dissection:**
- `docker exec -it ...`: Runs the command inside the running Kafka container.
- `kafka-topics.sh`: The built-in script for topic management.
- `--bootstrap-server localhost:9092`: Connects to the local broker.
- `--partitions 3`: Ensures we have multiple partitions to observe key hashing behavior.

### 2. Produce Messages with Headers
Let's produce a message that includes custom Headers (useful for Trace IDs).
```bash
docker exec -it <container_name_or_id> \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic producer-lab \
  --property "parse.headers=true" \
  --property "parse.key=true" \
  --property "key.separator=:" \
  --property "headers.separator=;"
```
*(Once started, type: `traceId:1234;user1:message payload` and hit enter).*

**Command Dissection:**
- `--property "parse.headers=true"`: Tells the CLI to expect headers in the input.
- `--property "parse.key=true"`: Tells the CLI to expect a key.
- The separators define how the CLI parses your raw string input.

## Code Execution

The Java implementation demonstrates three different producer profiles:
1. `DefaultProducer`: Focuses on Ack semantics.
2. `HighThroughputProducer`: Tweaks `batch.size` and `linger.ms`.
3. `IdempotentProducer`: Enforces strict exactly-once semantics per partition.

Run the test suite using Testcontainers to verify the behavior automatically:
```bash
mvn clean test
```

### 5. Simulating Resilience (Retries)
To manually observe the Idempotent Producer in action during a failure:
1. Run the `ResilienceRunner` in a separate terminal:
```bash
mvn exec:java -Dexec.mainClass="com.kafkalab.producer.ResilienceRunner"
```
2. While it sends messages every second, stop the Kafka container:
```bash
docker stop <container_name_or_id>
```
3. Observe the `ResilienceRunner` logs. It will log `NetworkException` and automatically retry sending the message without crashing, up to its `delivery.timeout.ms`.
4. Start the container again:
```bash
docker start <container_name_or_id>
```
5. Observe the producer recover and successfully resume sending messages!

## Cleanup
Once finished, tear down the infrastructure to free up ports and memory:
```bash
docker-compose down -v
```
**Command Dissection:**
- `down`: Stops and removes the containers.
- `-v`: Removes the associated anonymous and named volumes (wiping the Kafka data).

## Self-Assessment
<details>
<summary>1. What happens if <code>linger.ms</code> is set to 0? How does it affect throughput and latency?</summary>
The producer sends records immediately without waiting for the batch to fill up. This minimizes latency but drastically reduces throughput because network requests are less efficient and compression is barely utilized.
</details>

<details>
<summary>2. Why is <code>acks=all</code> recommended (and enforced) when <code>enable.idempotence=true</code>?</summary>
Idempotence guarantees no duplicates during retries. However, if <code>acks=1</code>, the leader could acknowledge, the producer moves on, and then the leader crashes before replicating. The message is lost, violating the durability guarantee that exact-once semantics rely upon. Thus, Kafka enforces <code>acks=all</code> to ensure the message is safely replicated before the sequence number advances.
</details>

<details>
<summary>3. How do message keys guarantee ordering, and what happens if the number of topic partitions changes dynamically?</summary>
Message keys are hashed (using murmur2 by default) and mapped to a specific partition. Since partitions are strictly ordered logs, all messages with the same key remain ordered. However, if you add more partitions to the topic later, the hash formula denominator changes, and new messages for the same key might land in a different partition, breaking the chronological ordering relative to older messages.
</details>
