# Concept: Kafka Connect & Change Data Capture

## Kafka Connect Architecture
Kafka Connect is a framework included in Apache Kafka that integrates Kafka with other systems. It is designed to be scalable and fault-tolerant. 

- **Connectors**: High-level logical jobs that coordinate data copying.
  - **Source Connectors**: Ingest data from external systems (e.g., Databases, APIs) into Kafka.
  - **Sink Connectors**: Export data from Kafka to external systems (e.g., Elasticsearch, S3).
- **Workers**: The JVM processes that execute Connectors and their Tasks.
  - **Standalone Mode**: A single worker process. Useful for development but lacks fault tolerance.
  - **Distributed Mode**: Multiple workers form a cluster. Provides automatic load balancing, fault tolerance, and a REST API for management.
- **Tasks**: The physical units of work. Connectors split their work into Tasks, which are distributed across the Workers.
- **Converters**: Code that translates data between Kafka Connect's internal data format and the serialized format in Kafka (e.g., Avro, JSON, Protobuf).
- **Transforms (SMTs)**: Single Message Transformations allow you to modify messages on the fly before they are written to Kafka (Source) or the target system (Sink).

## Change Data Capture (CDC) vs Polling
When integrating databases, a naive approach is to use a JDBC Source Connector that **polls** the database (e.g., `SELECT * FROM users WHERE updated_at > ?`).

**Disadvantages of Polling:**
- Adds read load to the database.
- Cannot easily capture hard `DELETE` operations (since the row is gone).
- Cannot capture multiple rapid updates to the same row between poll intervals.

**Change Data Capture (CDC)** (e.g., Debezium) solves this by tailing the database's internal transaction log (Write-Ahead Log or WAL in Postgres).
- **Advantages**: Minimal performance impact on the database, captures all intermediate state changes, and natively captures `DELETE` operations (represented as Kafka tombstones).

## Delivery Semantics in Connect
Kafka Connect provides **At-Least-Once** delivery by default. 
- Source connectors periodically commit their source offsets (e.g., the LSN in Postgres WAL) to a Kafka topic. If a worker crashes, it resumes from the last committed offset, potentially re-reading a few events but never losing data.
- Sink connectors track their consumer offsets, ensuring data is written to the target system before the offset is committed.
