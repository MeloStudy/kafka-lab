# LAB-007: Data Contracts & Schema Registry

## Infrastructure Dissection
This lab uses `docker-compose.yml` to provision:
- **Apache Kafka (KRaft)**: The event broker.
- **Bitnami Schema Registry**: An open-source compatible schema registry exposing the REST API on port `8081`.
- **Kafka-UI**: Observability tool on port `8080`.

## 1. Start the Infrastructure
```bash
docker-compose up -d
```

## 2. Exploring the Schema Registry REST API
Let's see if there are any subjects (schemas) registered yet:
```bash
curl -X GET \
  http://localhost:8081/subjects
```
*Command Dissection*: We use `-X GET` to explicitly state the HTTP method, hitting the `/subjects` endpoint to list all registered schema namespaces.
# Expected output: []

## 3. Maven Integration (Avro)
Notice that the `pom.xml` contains the `avro-maven-plugin`. This plugin reads the `.avsc` files in `src/main/avro` and generates Java POJOs during the compile phase.

```bash
# Generate the Java sources
mvn clean compile
```

## 4. Run the Producer & Consumer
Open two terminals.

**Terminal 1 (Consumer):**
```bash
mvn exec:java \
  -Dexec.mainClass="com.kafkalab.schemaregistry.AvroConsumer"
```
*Command Dissection*: We use Maven to execute the main class `AvroConsumer`, which reads from `orders-topic` and deserializes the Avro payload using `KafkaAvroDeserializer`.

**Terminal 2 (Producer):**
```bash
mvn exec:java \
  -Dexec.mainClass="com.kafkalab.schemaregistry.AvroProducer"
```
*Command Dissection*: This executes `AvroProducer`, which creates an `OrderV1` object, registers the schema with Schema Registry, and serializes the message.

Look back at Terminal 1. You should see the consumed `OrderV1` record!

## 5. Verify the Schema Registry
Now check the subjects in the Schema Registry:
```bash
curl -X GET \
  http://localhost:8081/subjects
```
*Command Dissection*: Fetches the list of subjects.
# Expected output: ["orders-topic-value"]

Fetch the specific schema for the subject:
```bash
curl -X GET \
  http://localhost:8081/subjects/orders-topic-value/versions/1
```
*Command Dissection*: Fetches version 1 of the schema for the `orders-topic-value` subject.


## 6. Schema Evolution (Integration Tests)
We have written TDD integration tests using Testcontainers to validate schema evolution compatibility. Run the tests:
```bash
mvn clean test
```
The tests demonstrate:
1. `OrderV1` serializes successfully.
2. `OrderV3` (which adds a field without a default) throws a `SerializationException` because it breaks `BACKWARD` compatibility.

<details>
<summary><b>Self-Assessment: Why is the Schema Registry a single point of failure during the initial schema resolution but not during steady-state streaming?</b></summary>
Because the Kafka clients (Producer and Consumer) cache the schema definitions locally in memory. Once they have mapped a Schema ID to the actual Schema structure, they no longer need to query the Schema Registry REST API for that specific ID.
</details>

<details>
<summary><b>Self-Assessment: If you add a new mandatory field (no default value) to an Avro schema, what type of compatibility does this break?</b></summary>
It breaks **BACKWARD** compatibility. Consumers using the new schema expect the mandatory field, but messages produced by older producers will not have it, causing deserialization failures.
</details>

<details>
<summary><b>Self-Assessment: How does the `KafkaAvroSerializer` embed the schema information in the Kafka message payload?</b></summary>
It uses a 5-byte header prefix in the binary payload: a "Magic Byte" (0x00) followed by a 4-byte Integer representing the Schema ID.
</details>
