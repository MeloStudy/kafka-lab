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
- **Auto Commit**: By default (`enable.auto.commit=true`), the consumer automatically commits the highest offset it has processed every 5 seconds. This provides **At-Least-Once** delivery semantics, but can lead to duplicate processing if the consumer crashes before committing.
- **Manual Commit**: Setting it to `false` requires you to call `consumer.commitSync()` manually. If you commit *before* processing a message and crash, you lose data (**At-Most-Once**). 

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
