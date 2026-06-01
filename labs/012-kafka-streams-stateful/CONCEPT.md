# Concept: Kafka Streams API - Stateful Processing

## KTable and GlobalKTable
In Kafka Streams, data can be interpreted in two ways:
1. **Stream (KStream)**: An infinite, append-only ledger of independent events.
2. **Table (KTable)**: A continuously updated materialized view representing the *latest* state of a given key. A KTable acts like an upsert log (changelog), where a newer event with the same key overwrites the older value.
3. **GlobalKTable**: Similar to KTable, but instead of data being partitioned across stream tasks, a complete copy of the *entire* table is broadcast to every single stream instance. Useful for small lookup tables.

## State Stores and RocksDB
Stateful operations (like aggregations, windowing, or joins) require a mechanism to remember past data. Kafka Streams provides this via **State Stores**. 
By default, Kafka Streams uses **RocksDB**, an embedded, high-performance, persistent key-value store. RocksDB runs locally on the same disk where the Stream application instance is deployed, providing sub-millisecond read/write access.

## Fault Tolerance: Changelog Topics
If a Stream application pod/instance crashes, its local RocksDB database is destroyed. To prevent data loss, Kafka Streams automatically creates internal **Changelog Topics** in the Kafka broker.
Whenever a state store is updated, the operation is also written to the changelog topic. When the replacement Stream thread starts, it first *replays* the changelog topic to reconstruct the local RocksDB state before it resumes processing new data.

## Repartitioning
Operations like `groupBy` (which changes the key of a stream) must ensure that all records with the new key end up on the same partition, so they can be processed by the same task and stored in the same local state store. Kafka Streams handles this by automatically creating an internal **Repartition Topic**, sending data to it, and reading it back with the new partitioning scheme.

## Windowing
Windowing allows us to group records that have the same key for stateful operations (like counting or aggregations) into time buckets.
- **Tumbling Windows**: Fixed-size, non-overlapping windows (e.g., every 1 minute).
- **Hopping Windows**: Fixed-size, overlapping windows (e.g., 5-minute window, advancing every 1 minute).
- **Session Windows**: Dynamically sized windows based on periods of activity separated by periods of inactivity.
