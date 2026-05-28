# LAB-003: The Consumer API

In this lab, we will write a vanilla Java Consumer to understand the mechanics of the `poll()` loop, horizontal scaling via Consumer Groups, and how Kafka handles slow consumers via Rebalances.

## Step 1: Provision the Infrastructure

Start the basic KRaft cluster:
```bash
docker-compose up -d
```

### 🔎 Infrastructure Dissection
The `docker-compose.yml` deploys a single-node KRaft broker and Kafka-UI on `http://localhost:8080`.

Let's create the topic we will use for this lab. We will explicitly give it **3 partitions**:
```bash
docker-compose exec kafka \
  kafka-topics.sh \
    --create \
    --topic lab003.events \
    --partitions 3 \
    --replication-factor 1 \
    --bootstrap-server localhost:9092
```
*Command Dissection:*
- `--create`: Instructs the script to create a new topic.
- `--topic`: The name of the topic.
- `--partitions`: Number of physical partitions (critical for our horizontal scaling demo).
- `--replication-factor`: Kept at 1 due to the single-node cluster.
- `--bootstrap-server`: The connection string for the internal broker listener.

## Step 2: Start the Dummy Producer

We need a continuous stream of events. We have provided `DummyProducer.java` which sends one event per second.

Run it in a separate terminal tab:
```bash
mvn compile exec:java -Dexec.mainClass="com.kafka.lab.DummyProducer"
```
*Command Dissection:*
- `mvn compile`: Compiles the Java project.
- `exec:java`: Uses the Exec Maven Plugin to run a Java class.
- `-Dexec.mainClass`: Specifies the fully qualified name of the main class to execute.

Keep this running in the background.

## Step 3: Run the Consumer

In a new terminal tab, run the `VanillaConsumer.java`:
```bash
mvn compile exec:java -Dexec.mainClass="com.kafka.lab.VanillaConsumer"
```
*(Same Maven dissection as above, but for the Consumer class).*

You will see it polling messages. Look at the console output; notice which `Partition` the consumer is reading from. Since it is the only consumer in the `lab003-group`, it is assigned all 3 partitions (0, 1, and 2).

## Step 4: Horizontal Scaling (Rebalances)

Let's see what happens when we scale out. 

Open **another** terminal tab, and run the consumer again (leaving the first one running):
```bash
mvn compile exec:java -Dexec.mainClass="com.kafka.lab.VanillaConsumer"
```
Watch the console logs of the **first** consumer. You will see a small pause. Kafka triggered a **Rebalance**. The partitions were redistributed. Now, Consumer 1 might be reading from Partitions 0 and 1, while Consumer 2 reads from Partition 2.

*Try starting a 3rd consumer! They will get exactly 1 partition each.*
*Try starting a 4th consumer! It will sit completely idle, waiting for one of the others to die.*

## Step 5: Simulating a Slow Consumer (max.poll.interval.ms)

What happens if a consumer's application logic freezes (e.g., a database timeout), but the process itself doesn't crash?

1. Stop all consumers (`Ctrl+C`).
2. Open `src/main/java/com/kafka/lab/VanillaConsumer.java` in your IDE.
3. Uncomment the `sleepFor(15000);` line inside the `for` loop.
4. Run the consumer again.

The consumer will process the first message and then sleep for 15 seconds. 
However, we configured `MAX_POLL_INTERVAL_MS_CONFIG` to `10000` (10 seconds).

**What happens?**
After 10 seconds, the Kafka Broker notices that the consumer hasn't called `poll()`. It assumes the consumer is deadlocked, **kicks it out of the group**, and triggers a Rebalance. 
When the consumer finally wakes up after 15 seconds and tries to commit its offset or fetch more data, it will crash with a `CommitFailedException` because it is no longer the owner of that partition!

## Step 6: Observability (Monitoring Consumer Lag)

To understand how far behind your consumers are, you can use the native CLI tools. 

First, describe the consumer group:
```bash
docker-compose exec kafka \
  kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group lab003-group
```
You will see a table displaying the `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG` for each partition. If `LAG` is high, your consumers are too slow!

You can also use the console consumer to join an existing group. This is useful for debugging what the group is seeing:
```bash
docker-compose exec kafka \
  kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic lab003.events \
    --group lab003-group
```
*Notice that when this CLI tool starts, it triggers a Rebalance and acts as another member of your consumer group.*

## Step 7: Clean Up

Stop your Java processes (`Ctrl+C`), and gracefully tear down the Kafka cluster:
```bash
docker-compose down -v
```

---

## 📝 Self-Assessment

<details>
<summary><b>1. What happens if you have 3 partitions and you start 4 consumers with the same `group.id`?</b></summary>
<br>
The 4th consumer will sit idle. Kafka never assigns a single partition to multiple consumers in the same group, as this would break message ordering and offset tracking guarantees.
</details>

<details>
<summary><b>2. Why does the Consumer have a separate heartbeat thread?</b></summary>
<br>
To decouple network liveness from processing speed. The heartbeat thread proves the consumer hasn't crashed (e.g. OOM or network partition). The `poll()` loop timeout proves the consumer hasn't deadlocked (e.g. infinite loop or frozen DB connection).
</details>

<details>
<summary><b>3. What is the danger of setting `enable.auto.commit=true` in a system that writes to a database?</b></summary>
<br>
If the consumer auto-commits the offset in the background, but then the application crashes *before* the data is successfully written to the database, that message is considered "processed" by Kafka. When the consumer restarts, it will pick up from the next offset. The unwritten message is permanently lost.
</details>
