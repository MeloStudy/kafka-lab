# CONCEPT: Delivery Semantics & Transactions

## 1. Delivery Semantics
In distributed messaging systems, delivery guarantees fall into three categories:
- **At-most-once**: Messages are sent once. If a failure occurs, the message is lost. (Low latency, data loss possible).
- **At-least-once**: Messages are retried until acknowledged. (No data loss, duplicates possible).
- **Exactly-once (EOS)**: Messages are delivered and processed exactly once, even in the event of failures. This is the hardest to achieve and requires specific protocols.

## 2. The Idempotent Producer
Kafka achieves exactly-once semantics by first introducing **Idempotence** (`enable.idempotence=true`).
- When a producer connects, it is assigned a **Producer ID (PID)**.
- Each message sent receives a monotonically increasing **Sequence Number**.
- If a network partition causes a producer to retry a message, the broker sees the same PID and Sequence Number and discards the duplicate.

## 3. The Transaction Coordinator & 2PC
Idempotence only guarantees exactly-once for a *single partition*. To guarantee atomic writes across *multiple* partitions (Consume-Process-Produce loop), Kafka uses the **Transaction Coordinator**.
- A producer configures a static `transactional.id`.
- The Transaction Coordinator manages a Two-Phase Commit (2PC) protocol.
- **Phase 1**: The producer writes data to target partitions. The data is written but marked as uncommitted.
- **Phase 2**: The producer asks the Coordinator to commit. The Coordinator writes commit "markers" (Control Messages) to the partitions.

## 4. Zombie Fencing (Producer Epochs)
If a network splits, you might end up with a "split-brain" scenario where two instances of the same producer are running (zombies).
- The `transactional.id` ties the PID to a specific logical producer.
- When a producer initializes (`initTransactions()`), it gets a new **Epoch**.
- If a zombie (with an older epoch) tries to commit, the Coordinator rejects it with a `ProducerFencedException`. This is known as **Zombie Fencing**.
