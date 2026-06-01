# LAB-012: Kafka Streams API - Stateful Processing

In this lab, we build a **stateful** streaming application that aggregates user clicks in real-time, using both global counts (`KTable`) and 1-minute tumbling windows.

Please read [CONCEPT.md](CONCEPT.md) to understand State Stores (RocksDB), Changelogs, and Windowing.

## Infrastructure
Start the KRaft cluster:
```bash
docker-compose up -d
```

Create the input and output topics:
```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh \
  --create --bootstrap-server localhost:9092 --topic user-clicks --partitions 3

docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh \
  --create --bootstrap-server localhost:9092 --topic user-clicks-total --partitions 3

docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh \
  --create --bootstrap-server localhost:9092 --topic user-clicks-windowed --partitions 3
```

## Step 1: Run the Application

Compile and run the Streams application from your terminal:
```bash
mvn compile exec:java -Dexec.mainClass="com.kafkalab.streams.StatefulStreamsApp"
```
The application uses Exactly-Once Semantics (`processing.guarantee="exactly_once_v2"`) to ensure that our counts are completely accurate even if there are restarts or crashes.

## Step 2: Produce Click Events

Open a console producer with a key-value format so we can set the user ID as the key:
```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic user-clicks \
  --property "parse.key=true" \
  --property "key.separator=:"
```
Send some clicks:
```text
alice:click
alice:click
bob:click
alice:click
```

## Step 3: Observe the Global KTable

In another terminal, consume the `user-clicks-total` topic. Because we are aggregating, we need to print both the Key and Value:
```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-clicks-total \
  --from-beginning \
  --property "print.key=true" \
  --property "key.separator=:"
```
You should see the count incrementing (e.g., `alice:3`, `bob:1`). This acts like an upsert log!

## Step 4: Explore Internal State Topics (Changelogs)

Our application used `Materialized.as("global-clicks-store")`. Let's see what Kafka Streams created behind the scenes:
```bash
docker exec -it <kafka-container-id> /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```
**Command Dissection**:
You will notice topics named `stateful-streams-app-global-clicks-store-changelog` and `stateful-streams-app-windowed-clicks-store-changelog`. These are the internal backup topics for the local RocksDB stores. If your app crashes, Kafka Streams will consume these topics to rebuild the exact counts.

## Self-Assessment
<details>
<summary>1. What is the fundamental difference between a <code>KStream</code> and a <code>KTable</code>?</summary>
A KStream is a stateless, append-only stream of independent events (like an insert ledger). A KTable is a stateful, continuously updated materialized view where newer events with the same key overwrite the older value (like a database table).
</details>

<details>
<summary>2. Why does <code>groupByKey()</code> followed by an aggregation often create an internal "repartition" topic?</summary>
Aggregations require all data for a specific key to go to the same Task (so it can be stored in the same local RocksDB partition). If the upstream key was changed or if the partition count doesn't match, Kafka Streams automatically writes the data to an internal repartition topic to shuffle the data correctly before aggregation.
</details>

<details>
<summary>3. If the local disk fails and the RocksDB state store is corrupted, how does Kafka Streams recover?</summary>
Kafka Streams automatically deletes the corrupted local store and rebuilds it by replaying all messages from the internal changelog topic associated with that state store.
</details>
