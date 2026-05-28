# LAB-006: Advanced Consumer Patterns

Welcome to the Advanced Consumer Patterns lab. Here we transition from "Hello World" consumers to enterprise-ready consumers that handle manual offsets and dynamic rebalances safely.

## 1. Environment Setup

If you haven't already, start your infrastructure. This lab requires at least a basic KRaft broker.

```bash
cd docs/templates/infra
docker-compose -f docker-compose-basic.yml up -d
```

> **Infrastructure Dissection (Basic KRaft)**:
> This setup runs Apache Kafka without Zookeeper. The broker itself acts as the KRaft controller (`process.roles=broker,controller`). The internal storage for metadata is kept in a local log directory rather than an external distributed system.

## 2. Compiling the Code

Navigate back to the root of the lab and compile the module.

```bash
mvn clean package -pl :006-advanced-consumer-patterns -DskipTests
```

## 3. Manual Offset Commits

Run the `ManualCommitConsumer` (you can do this in your IDE by executing the `ManualCommitConsumer` with a `main` method, or running the tests). 

Because we use `enable.auto.commit=false`, we control when offsets are committed. 

### Observing Consumer Lag

To see how offsets are tracked, use the CLI while your consumer is running or after it finishes:

```bash
docker-compose exec kafka \
  kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group my-manual-group
```

> **Command Dissection**:
> - `kafka-consumer-groups.sh`: The tool to inspect group state, offsets, and lag.
> - `--describe`: Shows detailed partition-level metrics.
> - `--group my-manual-group`: The specific group we want to inspect.
> - **Output**: You will see `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG`. If lag is 0, the consumer is caught up.

## 4. Rebalance Listener & Standby Consumers

The `RebalanceListenerConsumer` demonstrates how to save state and commit offsets when partitions are yanked away.

### Triggering a Rebalance

1. Create a topic with **3 partitions**:
```bash
docker-compose exec kafka \
  kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic rebalance-topic \
  --partitions 3
```

2. Start **one** instance of `RebalanceListenerConsumer`. It will be assigned all 3 partitions.
3. Start a **second** instance. You will see `onPartitionsRevoked` trigger in the first instance, followed by a rebalance where partitions are split (e.g., 2 and 1).
4. Start a **third** instance. Each gets 1 partition.
5. Start a **fourth** instance. The fourth instance becomes a **Standby Consumer** and gets 0 partitions.
6. Stop the first instance. You will see a rebalance occur, and the standby consumer will suddenly be assigned a partition.

## Self-Assessment

<details>
<summary>1. Why is <code>commitAsync()</code> generally preferred over <code>commitSync()</code> in the main <code>poll()</code> loop, and when is <code>commitSync()</code> absolutely necessary?</summary>

`commitAsync()` does not block the thread waiting for a broker response, allowing the consumer to process the next batch immediately (high throughput). However, it does not retry on failure. `commitSync()` is necessary during shutdown or in `onPartitionsRevoked()` to guarantee offsets are saved before the consumer exits or loses the partition.
</details>

<details>
<summary>2. What happens if a rebalance occurs before you have committed the offsets for the currently processing batch?</summary>

The partitions may be reassigned to another consumer. Because the offsets weren't committed, the new consumer will read the messages from the last committed offset, leading to duplicate processing. This is why a `ConsumerRebalanceListener` is critical to commit pending offsets before revocation.
</details>

<details>
<summary>3. What is the difference between <code>onPartitionsRevoked()</code> and <code>onPartitionsLost()</code>?</summary>

`onPartitionsRevoked()` is called during a graceful rebalance, giving you time to commit offsets. `onPartitionsLost()` is called in exceptional cases (e.g., the consumer lost its connection to the coordinator and missed heartbeats). In the "lost" scenario, the coordinator has *already* given the partitions to someone else, so you cannot commit offsets anymore; you can only clean up local state.
</details>
