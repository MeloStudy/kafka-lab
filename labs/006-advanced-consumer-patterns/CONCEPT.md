# CONCEPT: Advanced Consumer Patterns

## 1. Offset Management Internals

Kafka does not track which messages you have read. Instead, the **consumer** is responsible for keeping track of its own offset. It does this by periodically writing the offset to a special internal Kafka topic called `__consumer_offsets`.

### Auto vs Manual Commits
- **Auto-Commit** (`enable.auto.commit=true`): A background thread automatically commits the offset every `auto.commit.interval.ms` (default 5 seconds). If your consumer crashes *after* polling but *before* fully processing the batch, the background thread might have committed the offset anyway. This leads to **message loss** (At-Most-Once semantics).
- **Manual Commit** (`enable.auto.commit=false`): You explicitly tell Kafka when you are done. If you commit *after* processing, you guarantee **At-Least-Once** semantics.

### Synchronous vs Asynchronous Commits
- `commitSync()`: Blocks the calling thread until the broker responds. If it fails, it will automatically retry. This is safe but slow, as it halts the `poll()` loop.
- `commitAsync()`: Sends the commit request and immediately returns. It does **not** retry on failure (because a later commit might have already succeeded). This allows for high throughput.

**Best Practice**: Use `commitAsync()` in the main `poll()` loop for speed, and wrap your shutdown logic in a `finally` block with a `commitSync()` to ensure the last offset is safely stored.

## 2. The Rebalance Protocol

When consumers join or leave a consumer group (or when a topic's partitions change), Kafka must re-distribute the partitions among the consumers. This is called a **Rebalance**.

### Rebalance Lifecycle:
1. **JoinGroup**: All consumers send a JoinGroup request to the Group Coordinator.
2. **SyncGroup**: The coordinator selects a "Group Leader" (usually the first consumer to join). The leader calculates the partition assignment and sends it back to the coordinator. The coordinator distributes it to the members.
3. **Heartbeats**: Consumers must send heartbeats to the coordinator. If `session.timeout.ms` is exceeded without a heartbeat, the coordinator assumes the consumer is dead and triggers a rebalance.

### The Problem with Rebalances
During a rebalance, consumers must stop reading, and partitions are shifted around. If you were in the middle of processing a large batch of messages but haven't committed the offsets yet, those partitions might be given to another consumer. That new consumer will read the *same* messages from the last committed offset, leading to duplicate processing.

## 3. ConsumerRebalanceListener

To handle the chaos of rebalances gracefully, you can attach a `ConsumerRebalanceListener` to your subscription.

- `onPartitionsRevoked(Collection<TopicPartition> partitions)`: Called *before* the rebalance finishes. This is your last chance to commit offsets for the partitions you are about to lose or to save any local state to an external database.
- `onPartitionsAssigned(Collection<TopicPartition> partitions)`: Called *after* the rebalance finishes. Use this to load state for the newly assigned partitions.

## 4. Standby Consumers

A topic has a fixed number of partitions (e.g., 5). If your consumer group has 5 consumers, each gets 1 partition. If you start a 6th consumer, it will be assigned **0 partitions**. It sits idle as a "Standby Consumer." If one of the 5 active consumers crashes, a rebalance occurs, and the standby consumer is immediately assigned the abandoned partition, ensuring high availability.
