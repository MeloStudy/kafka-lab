# LAB-002: The Producer API

## Overview
This lab covers the internal mechanics of the Kafka Producer API, including Acks, Retries, Batching, Compression, Idempotence, Message Keys, Headers, and Topic Auto-Creation.

> **Status**: `[DONE]`

## Infrastructure Setup

To begin, start the local KRaft cluster.
```bash
docker-compose up -d
```

### 🔎 Infrastructure Dissection
The `docker-compose.yml` uses Kafka in KRaft mode (no Zookeeper).
- `KAFKA_PROCESS_ROLES=controller,broker`: This single container acts as both the data broker and the cluster controller.
- `KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092...`: We configure internal routing for Docker-to-Docker communication (29092) and external for your host machine (9092).
- `KAFKA_CONTROLLER_QUORUM_VOTERS=0@kafka:9093`: Defines the quorum. Since it's a single node, it votes for itself.

## Hands-On CLI Practice

Before writing Java code, let's test using the CLI.

### 1. The Dangers of Topic Auto-Creation
What happens if you produce to a topic that doesn't exist? Let's try it:
```bash
docker-compose exec kafka \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic auto-created-topic
```
*(Type a message and hit enter. Press Ctrl+C to exit).*

Now, let's inspect the topic that Kafka automatically created:
```bash
docker-compose exec kafka \
  kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic auto-created-topic
```
**Observation**: It likely created it with only 1 partition! This is why `auto.create.topics.enable` should be `false` in production. Always create topics explicitly.

### 2. Create a Proper Topic
Create a topic explicitly with 3 partitions to test message keys.
```bash
docker-compose exec kafka \
  kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic producer-lab \
  --partitions 3
```

### 3. Produce Messages with Acks
Let's produce a message explicitly demanding `acks=all` (full quorum acknowledgment) before considering it successful.
```bash
docker-compose exec kafka \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic producer-lab \
  --producer-property acks=all
```
*(Type a message and hit enter. Press Ctrl+C to exit).*

**Command Dissection:**
- `--producer-property acks=all`: Injects the `acks` configuration directly into the underlying Java producer used by the CLI.

### 4. Produce Messages with Keys
By default, messages without keys are sent round-robin. Let's send a message with a specific key to guarantee it lands in a specific partition.
```bash
docker-compose exec kafka \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic producer-lab \
  --property "parse.key=true" \
  --property "key.separator=:"
```
*(Type exactly: `user1:my event payload` and hit enter. Press Ctrl+C to exit).*

**Command Dissection:**
- `--property "parse.key=true"`: Tells the CLI to expect a key.
- `key.separator=:`: Everything before the `:` is the key (`user1`), everything after is the value.

### 5. Produce Messages with Headers
Headers are great for passing metadata (like Trace IDs) without touching the payload.
```bash
docker-compose exec kafka \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic producer-lab \
  --property "parse.headers=true" \
  --property "parse.key=true" \
  --property "key.separator=:" \
  --property "headers.separator=;"
```
*(Type exactly: `traceId:1234;user1:my event payload` and hit enter. Press Ctrl+C to exit).*

**Command Dissection:**
- `--property "parse.headers=true"`: Tells the CLI to expect headers in the input.
- `headers.separator=;`: Everything before the `;` is the header (`traceId:1234`), everything after is the key/value.

## Code Execution

The Java implementation demonstrates three different producer profiles:
1. `DefaultProducer`: Focuses on Ack semantics.
2. `HighThroughputProducer`: Tweaks `batch.size` and `linger.ms`.
3. `IdempotentProducer`: Enforces strict exactly-once semantics per partition.

### 6. Unit Testing (MockProducer)
We use `MockProducer` to unit test our logic without needing a real broker (Integration testing with real brokers is covered in LAB-004!).
```bash
mvn clean test
```

### 7. Simulating Resilience (Retries)
To manually observe the Idempotent Producer in action during a failure:
1. Run the `ResilienceRunner` in a separate terminal:
```bash
mvn exec:java -Dexec.mainClass="com.kafkalab.producer.ResilienceRunner"
```
2. While it sends messages every second, stop the Kafka container:
```bash
docker-compose stop kafka
```

**Command Dissection:**
- `docker-compose stop kafka`: Sends a SIGTERM to the specific 'kafka' service container defined in the compose file, simulating a broker crash or network partition.

3. Observe the `ResilienceRunner` logs. It will log `NetworkException` and automatically retry sending the message without crashing, up to its `delivery.timeout.ms`.

4. Start the container again:
```bash
docker-compose start kafka
```

**Command Dissection:**
- `docker-compose start kafka`: Brings the broker back online, simulating network recovery.

5. Observe the producer recover and successfully resume sending messages!

### 8. Simulating Backpressure (BufferExhaustedException)
Kafka producers hold messages in memory before sending them over the network. The maximum memory allocated for this is `buffer.memory`. If you produce messages faster than they can be sent (e.g., due to network latency or broker overload), the buffer will fill up.
When the buffer is full, the producer will block (wait) up to `max.block.ms`. If space is still not available after that time, it will throw a `BufferExhaustedException` (or `TimeoutException` in newer clients).

To simulate this locally, we have a runner that intentionally limits the buffer size and blocks for a very short time:
```bash
mvn exec:java -Dexec.mainClass="com.kafkalab.producer.BackpressureRunner"
```
You will observe the producer rapidly queuing messages until it runs out of memory and throws the exception.

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
