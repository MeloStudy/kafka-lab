# CONCEPT: Data Contracts & Schema Registry

## The Problem with Untyped JSON
In an Event-Driven Architecture, the producer and consumer are completely decoupled. If a producer changes the shape of the data (e.g., renames `productId` to `item_id`), the consumer will break without warning. Untyped JSON strings provide no safety guarantees. We need a "Data Contract."

## Serialization Formats
- **JSON**: Human readable, but verbose and untyped.
- **Protobuf**: Highly efficient binary format by Google. Excellent for gRPC.
- **Avro**: Binary format by Apache. Excellent for Kafka because the schema is stored separately from the message, reducing payload size significantly. Kafka ecosystem (Confluent) natively integrates with Avro.

## Confluent Schema Registry Internals
When a producer wants to send an Avro message:
1. It connects to the Schema Registry.
2. It sends the Avro Schema.
3. The Registry checks if the schema exists for the topic (Subject). If not, it assigns a new **Schema ID** and stores the schema in a compacted internal Kafka topic called `_schemas`.
4. The Producer caches this Schema ID.
5. The Producer serializes the message: `[Magic Byte (0)] + [Schema ID (4 bytes)] + [Binary Avro Payload]`.
6. The Consumer reads the message, extracts the Schema ID, fetches the schema from the Schema Registry (and caches it), and deserializes the binary payload.

## Schema Evolution
Schemas must evolve as business needs change. The Schema Registry enforces compatibility rules:
- **BACKWARD**: Consumers using the NEW schema can read data produced with the OLD schema. (Rule: You can only delete fields or add optional fields with defaults).
- **FORWARD**: Consumers using the OLD schema can read data produced with the NEW schema. (Rule: You can only add fields or delete optional fields).
- **FULL**: Both Backward and Forward compatible.

## Resilience (Schema Registry Down)
If the Schema Registry goes down *after* the producer and consumer have successfully retrieved/registered the schema, the system **continues to work**. The `KafkaAvroSerializer` and `KafkaAvroDeserializer` cache the schemas locally in memory. The registry is only a single point of failure during the initial schema resolution (cache miss).
