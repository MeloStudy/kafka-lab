# LAB-011: Kafka Streams API - Stateless Processing

In this lab, we dive into the **Kafka Streams DSL** to process data in real-time. We will build a topology that filters and masks a stream of transactions.

Before proceeding, review [CONCEPT.md](CONCEPT.md) for theory on topologies, KStreams, and stateless operations.

## Infrastructure Dissection
We are using a basic KRaft infrastructure.
- **Kafka**: KRaft-based broker (`apache/kafka:4.3.0`).
- **Kafka-UI**: Web interface at `http://localhost:8080`.

## Step 1: Start Infrastructure & Create Topics

Start the cluster:
```bash
docker-compose up -d
```

Create the required input and output topics:
```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic raw-transactions \
  --partitions 3
```

```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic high-value-transactions \
  --partitions 3
```

**Command Dissection**:
- `--partitions 3`: We create 3 partitions so that our Streams application can potentially scale horizontally up to 3 threads/instances.

## Step 2: Run the Kafka Streams Application

Compile and run the Streams application from your IDE or terminal. 
If running from the terminal:
```bash
mvn compile exec:java -Dexec.mainClass="com.kafkalab.streams.StatelessStreamsApp"
```
You will see the application print its `Topology Description` and then start running continuously.

## Step 3: Produce Test Data

Open a new terminal and start a console producer to send JSON payloads to the `raw-transactions` topic.

```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic raw-transactions
```

Paste the following events one by one:
```json
{"amount": 500.0, "user": "Alice", "creditCard": "1111-2222-3333-4444"}
{"amount": 1500.0, "user": "Bob", "creditCard": "5555-6666-7777-8888"}
{"amount": 2500.0, "user": "Charlie", "creditCard": "9999-0000-1111-2222"}
```

## Step 4: Verify the Filtered Stream

Open another terminal and consume from the output topic:

```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic high-value-transactions \
  --from-beginning
```

You should see:
```json
{"amount":1500.0,"user":"Bob","creditCard":"****-****-****-8888"}
{"amount":2500.0,"user":"Charlie","creditCard":"****-****-****-2222"}
```
*Notice that Alice's transaction ($500) was dropped by the `filter`, and the `creditCard` fields were masked by the `mapValues` operation.*

## Self-Assessment
<details>
<summary>1. Why is <code>TopologyTestDriver</code> preferred over Testcontainers for unit testing Streams topologies?</summary>
<code>TopologyTestDriver</code> executes the topology locally within the same JVM thread, entirely bypassing network calls and brokers. It is blazingly fast, deterministic, and doesn't require Docker or heavy infrastructure, making it ideal for standard unit tests of business logic.
</details>

<details>
<summary>2. If your input topic has 6 partitions, and you configure <code>num.stream.threads=2</code> across 2 application instances (4 threads total), how are tasks distributed?</summary>
Kafka Streams creates exactly 6 Tasks (one per partition). These 6 Tasks will be balanced across the 4 active Stream Threads. For example, two threads will get 2 tasks each, and two threads will get 1 task each.
</details>

<details>
<summary>3. What is the difference between <code>map</code> and <code>mapValues</code>, and why is <code>mapValues</code> preferred when possible?</summary>
<code>map</code> allows changing both the Key and the Value, which forces Kafka Streams to assume the partitioning strategy might be invalidated, triggering an expensive data repartitioning (writing to an internal topic and reading back). <code>mapValues</code> only changes the Value, keeping the Key intact, so Kafka Streams knows repartitioning is unnecessary.
</details>
