# Concepts: The Producer API

To master the Kafka Producer, you must move beyond the `send()` method and understand the background threads and configurations that govern throughput, latency, and reliability.

## 1. The Acknowledgement Spectrum (`acks`)
When a producer sends a record, it requires a guarantee from the broker that the data was safely received.
- `acks=0`: **Fire and Forget**. The producer does not wait for a response. Max throughput, highest chance of data loss.
- `acks=1`: **Leader Acknowledgment**. The producer waits for the partition Leader to write the record to its local log. If the leader crashes before replicating to followers, data is lost.
- `acks=all` (or `-1`): **Full Quorum Acknowledgment**. The leader waits for all in-sync replicas (ISRs) to acknowledge the record. Highest durability, lower throughput. *Note: In Kafka 3.0+, `acks=all` is the default.*

## 2. Batching for Throughput (`batch.size` & `linger.ms`)
Kafka producers don't send messages instantly one-by-one. They accumulate them into batches.
- `batch.size`: The maximum size (in bytes) of a single batch.
- `linger.ms`: The maximum time to wait for a batch to fill up before sending it.
If `linger.ms` is 0 (historic default), the producer sends immediately. If you want higher throughput, increase `linger.ms` (e.g., 20ms) and `batch.size` to allow more messages to be compressed and sent together.

## 3. Compression (`compression.type`)
Kafka supports `gzip`, `snappy`, `lz4`, and `zstd`. Compression is applied to the **entire batch**, not individual messages. Larger batches compress better, saving network bandwidth and broker storage at the cost of slight CPU overhead on the producer and consumer.

## 4. The Idempotent Producer (`enable.idempotence=true`)
When a network error occurs, the producer automatically retries. But what if the broker actually received the message, and only the ACK was lost? Retrying would create a duplicate.
- **Idempotence** solves this by attaching a Producer ID (PID) and a Sequence Number to each message.
- The broker tracks the highest sequence number for each PID-Partition pair and discards duplicates.
- *Note: In Kafka 3.0+, idempotence is enabled by default.*

## 5. Message Keys & Partitioning
By default, Kafka uses a **Sticky Partitioner** for records without keys, batching records to one partition at a time to optimize throughput.
However, if a `Key` is provided (e.g., `user-id-123`), Kafka hashes the key and guarantees that all messages with the same key will ALWAYS land in the same partition. This guarantees strict chronological ordering for that specific entity.
