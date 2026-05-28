# The Consumer API: Core Concepts

While producing messages is relatively straightforward, consuming messages in a distributed system introduces complex coordination challenges. Kafka's Consumer API handles this complexity behind the scenes.

## 1. Consumer Groups & Horizontal Scaling
A **Consumer Group** (defined by `group.id`) is a logical group of consumers that cooperate to consume data from a topic.
- Kafka distributes the partitions of a topic evenly among the active consumers in a group.
- **The Golden Rule of Scaling**: You cannot have more active consumers in a group than the number of partitions. If a topic has 3 partitions and you start 4 consumers in the same group, the 4th consumer will sit idle.
- If you start a consumer with a *different* `group.id`, it acts independently and receives a full copy of the data.

## 2. Offsets and `__consumer_offsets`
Consumers need to remember where they left off in case they crash. They do this by "committing" their current offset back to Kafka.
- Kafka stores these commits in an internal, compacted topic called `__consumer_offsets`.

### Delivery Semantics & Committing
The timing of *when* a consumer commits its offset defines the **Delivery Semantics** of your application:
- **At-Least-Once (Default)**: If `enable.auto.commit=true` (or if you manually commit *after* processing), Kafka ensures the message is processed. However, if the consumer crashes after processing but *before* committing, the new consumer will re-read and re-process the message, leading to duplicates. 
- **At-Most-Once**: If you manually commit (`enable.auto.commit=false`) *before* processing the message and then crash, the new consumer will start after that offset. The message is permanently lost.
- **Exactly-Once**: Requires Kafka Transactions (covered in later labs).

### Consumer Lag
**Consumer Lag** is one of the most critical observability metrics in Kafka. It is the difference between the latest offset produced to a partition (Log End Offset) and the latest offset committed by the consumer group (Current Offset).
- High lag means your consumers are falling behind the producers.
- You can monitor lag using native CLI tools or metrics platforms like Prometheus/Datadog.

### Rewinding Offsets (Administrative Control)
Offsets are not permanent once committed. As an administrator, you have the ability to manually overwrite a consumer group's offsets.
- **Rewinding**: Setting the offset back to an earlier point to re-process data (e.g., after fixing a bug in your consumer logic).
- **Fast-forwarding**: Setting the offset forward to skip over data (e.g., skipping a "poison pill" message that is crashing consumers).
- *Rule:* The consumer group MUST be inactive (all consumer processes stopped) before you can administratively change its offsets.

## 3. The `poll()` Loop Mechanics
The core of every consumer is the infinite `poll()` loop. This loop does two crucial things:
1. It fetches data from the broker.
2. It acts as a liveness signal (Heartbeat).

### The Heartbeat Thread vs Processing Thread
Modern Kafka consumers use two threads:
- **Foreground Thread**: The thread where your `poll()` loop runs. It fetches data and processes it.
- **Background Heartbeat Thread**: Once `poll()` is called the first time, a background thread starts sending lightweight heartbeats to the broker (governed by `session.timeout.ms`). This proves the consumer hasn't crashed (Network Death).

### Flow Control: `max.poll.interval.ms`
What if the consumer hasn't crashed, but is just taking a very long time to process a message (e.g., slow database)? The heartbeat thread would keep telling the broker "I am alive", but no progress is being made.
To prevent this, Kafka enforces `max.poll.interval.ms` (default 5 minutes). If the foreground thread does not call `poll()` again within this time, the broker assumes the consumer is "deadlocked" or "livelocked" and kicks it out of the group.

## 4. The Rebalance Protocol
When a consumer joins a group, leaves a group, or gets kicked out, Kafka triggers a **Rebalance**.
A Rebalance is the process of re-assigning partitions among the remaining active consumers.
- **Eager Rebalance**: The legacy approach. All consumers drop their current partitions (Stop-The-World), and partitions are reassigned from scratch.
- **Cooperative Sticky Assignor**: The modern approach. Only the affected partitions are moved, minimizing disruption to consumers that didn't crash.
