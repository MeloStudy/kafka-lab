# LAB-005: Delivery Semantics & Transactions

## Infrastructure Dissection

This lab uses the `docker-compose.yml` to spin up a single KRaft broker.
When enabling transactions in a single-broker setup, you MUST override the default replication factors for the internal transaction state topics:
- `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`: Default is 3, but we only have 1 broker.
- `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`: Minimum in-sync replicas needed to accept a transaction state update.

## Command Dissection: Verifying Isolation Levels

1. Start the infrastructure:
```bash
docker-compose up -d
```

2. Open two terminal windows.
In Terminal 1 (Read Uncommitted - Default):
```bash
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions-topic \
  --from-beginning
```
*Notice:* This consumer will see ALL messages, including aborted ones.

In Terminal 2 (Read Committed):
```bash
docker-compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions-topic \
  --from-beginning \
  --isolation-level read_committed
```
*Notice:* The `--isolation-level read_committed` flag tells the consumer to buffer messages until it sees a Commit Marker. It will skip over messages that have an Abort Marker.

## Java Implementation
Review the native Kafka Client code in `src/main/java/com/melostudy/kafka/delivery/`:
- `TransactionalProducer.java`: Demonstrates `beginTransaction()` and `commitTransaction()`.
- `ReadCommittedConsumer.java`: Demonstrates setting `isolation.level`.

Run the tests to see Zombie Fencing in action:
```bash
mvn clean test
```

## Self-Assessment
<details>
<summary>1. How does the Transaction Coordinator ensure atomic commits across multiple partitions?</summary>
It uses a Two-Phase Commit protocol. First, data is written to the partitions. Then, the Coordinator writes invisible Control Messages (markers) to all partitions involved to signal whether the transaction was committed or aborted.
</details>

<details>
<summary>2. What happens if a consumer reads with `read_uncommitted` while a transaction is aborted?</summary>
The consumer will read the messages as they are appended to the log, completely ignoring the transaction abort marker, leading to processing of invalid/aborted data.
</details>

<details>
<summary>3. How does `transactional.id` prevent the "zombie producer" problem?</summary>
By tying a static ID to an Epoch. If a new instance spins up with the same `transactional.id`, the Epoch increments. The Transaction Coordinator will then fence off (reject) any commit requests from the old instance (which has an older Epoch), preventing split-brain writes.
</details>
