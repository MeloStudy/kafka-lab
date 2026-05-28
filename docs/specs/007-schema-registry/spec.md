# Spec: LAB-007: Data Contracts & Schema Registry
**Status**: `[DRAFT]`

## 1. Pedagogical Objectives
- Understand the importance of Data Contracts and schema management in Event-Driven Architectures.
- Master Avro serialization and its integration with Kafka.
- Understand Confluent Schema Registry internals (how schemas are stored, ID mapping).
- Master Schema Evolution and compatibility rules (Backward, Forward, Full, Transitive).

## 2. Core Concepts (CONCEPT.md)
- **Data Governance**: Why untyped JSON is dangerous in distributed streams.
- **Serialization Formats**: Avro vs Protobuf vs JSON Schema. Focus on Avro.
- **Schema Registry Internals**: The `_schemas` internal topic, REST API, client-side caching.
- **Schema Evolution**: 
  - Backward Compatibility (Consumers can read old data).
  - Forward Compatibility (Consumers can read new data).
  - Full Compatibility.

## 3. Infrastructure & Tooling
- **Infrastructure Profile**: Option B: Schema Registry included (KRaft + Confluent Schema Registry).
- **CLI Commands**:
  - `kafka-avro-console-producer`
  - `kafka-avro-console-consumer`
  - `curl` commands to interact directly with the Schema Registry REST API (e.g., `GET /subjects`, `POST /subjects/Topic-value/versions`).

## 4. Practical Implementation (README.md)
- **Step 1: Schema Definition**: Define an `Order` Avro schema (`order-v1.avsc` with `orderId`, `productId`, `amount`).
- **Step 2: Maven Integration**: Use `avro-maven-plugin` to generate Java classes from the schema.
- **Step 3: CLI Testing**: Start infra, register the schema via REST API, produce and consume Avro messages via CLI.
- **Step 4: Java Client Implementation**: Write a Kafka Producer using `KafkaAvroSerializer` and a Consumer using `KafkaAvroDeserializer`.
- **Step 5: Schema Evolution**: Define `order-v2.avsc` (add a `loyaltyPoints` int field with default `0` for BACKWARD compatibility) and demonstrate successful evolution. Attempt to register `order-v3.avsc` (add `discount` field with NO default) and demonstrate compatibility failure.

## 6. TDD & Technical Verification
- **Test 1: Producer Test**: Verify `KafkaAvroSerializer` correctly registers and serializes an `Order` object using Testcontainers (Kafka + Schema Registry container).
- **Test 2: Schema Evolution Validation**: Verify that attempting to register an incompatible schema throws a `RestClientException` or `SerializationException` in a test environment.
- **Test 3: Consumer Test**: Verify consumer can successfully deserialize an event with the schema retrieved from the registry.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: At-Least-Once delivery.
- **Resilience Scenario (Schema Registry Down)**: Demonstrate that the Producer and Consumer can still function if the Schema Registry goes down *after* the initial schema resolution, thanks to the client-side schema cache (`schema.registry.url` local caching). What happens on cache miss?

## 7. Self-Assessment Questions
1. Why is the Schema Registry a single point of failure during the initial schema resolution but not during steady-state streaming?
2. If you add a new mandatory field (no default value) to an Avro schema, what type of compatibility does this break?
3. How does the `KafkaAvroSerializer` embed the schema information in the Kafka message payload? (Hint: Magic Byte + Schema ID).
