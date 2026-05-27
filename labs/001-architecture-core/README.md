# LAB-001: Architecture Core & Infrastructure

Welcome to your first practical lab! In this module, we will provision a Kafka cluster and use the native CLI tools to interact with it.

Please ensure you have read the theory in [`CONCEPT.md`](CONCEPT.md) before starting.

## Step 1: Provision the Cluster

We are going to deploy a single-node KRaft broker alongside **Kafka-UI** for visual observability. 

1. Open your terminal in this directory (`labs/001-architecture-core`).
2. Run the following command to start the infrastructure in the background:
   ```bash
   docker-compose up -d
   ```
3. Open your browser and navigate to `http://localhost:8080`. You should see the Provectus Kafka-UI dashboard. Verify that the `local-kraft` cluster is online and healthy.

### 🔎 Infrastructure Dissection: The KRaft Configuration
If you look at the `docker-compose.yml`, you will notice several critical environment variables that configure our node natively (without Zookeeper):
- `KAFKA_PROCESS_ROLES=controller,broker`: In KRaft, a node can store user data (`broker`), vote on cluster metadata (`controller`), or both. We are running a combined node.
- `KAFKA_LISTENERS`: Defines the network interfaces Kafka binds to. We use `INTERNAL` (for broker-to-broker traffic), `EXTERNAL` (for your future Java apps), and `CONTROLLER` (for KRaft quorum voting).
- `KAFKA_CONTROLLER_QUORUM_VOTERS=0@kafka:9093`: Tells the broker who the voting controllers are. Since it is a single-node cluster, it votes for itself (Node ID 0).

## Step 2: Access the CLI Tools

Kafka ships with a suite of bash scripts to manage the cluster natively. Since we are using Docker, these scripts are located inside the Kafka container.

To get an interactive shell inside the Kafka broker container, run:
```bash
docker exec -it 001-architecture-core-kafka-1 bash
```
*(Note: If your container is named differently, use `docker ps` to find the exact name).*

## Step 3: Managing Topics (`kafka-topics.sh`)

Once inside the container, we will create our first topic.

1. **Create a topic** called `lab001.events` with 3 partitions and a replication factor of 1:
   ```bash
   kafka-topics.sh \
     --create \
     --topic lab001.events \
     --partitions 3 \
     --replication-factor 1 \
     --bootstrap-server localhost:9092
   ```
   *Command Dissection:*
   - `--partitions 3`: Divides the topic into 3 physical directories on disk, allowing up to 3 consumers to process data in parallel later.
   - `--replication-factor 1`: We only keep 1 copy of the data because we have a single broker.
   > 💡 **What about defaults?** If you omit `--partitions` or `--replication-factor`, Kafka will fall back to the broker's default configurations (`num.partitions` and `default.replication.factor`, which are typically both `1` out of the box). It is a best practice to **always** specify them explicitly to avoid surprises.

2. **List all topics** to verify it was created:
   ```bash
   kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

3. **Describe the topic** to see its physical topology:
   ```bash
   kafka-topics.sh --describe --topic lab001.events --bootstrap-server localhost:9092
   ```
   > Look closely at the output. For each partition (0, 1, and 2), it will tell you which broker is the `Leader`, and the `Isr` (In-Sync Replicas) list. Since we only have one broker (Node ID 0), it is both the leader and the only member of the ISR.

4. **Alter the topic** to increase the number of partitions to 4:
   ```bash
   kafka-topics.sh \
     --alter \
     --topic lab001.events \
     --partitions 4 \
     --bootstrap-server localhost:9092
   ```

5. **Describe the topic again** to verify the new partition was created:
   ```bash
   kafka-topics.sh --describe --topic lab001.events --bootstrap-server localhost:9092
   ```

## Step 4: The Producer & Consumer Flow

We will now simulate data flowing through the system. We need two separate terminal windows for this.

**Terminal 1 (The Producer)**
Exec into the container and start the console producer:
```bash
docker exec -it 001-architecture-core-kafka-1 bash

# Start pushing messages to the topic
kafka-console-producer.sh \
  --topic lab001.events \
  --bootstrap-server localhost:9092
```
Type a few messages (e.g., "Hello Kafka", "Event 2") and hit Enter after each.

**Terminal 2 (The Consumer)**
Exec into the container in a new tab and start the console consumer:
```bash
docker exec -it 001-architecture-core-kafka-1 bash

# Start consuming messages
kafka-console-consumer.sh \
  --topic lab001.events \
  --bootstrap-server localhost:9092
```

Notice that the consumer **did not** print the messages you just sent! Why? Because by default, a new consumer starts reading from the *tail* of the log (new messages only). 

To read historical data, stop the consumer (`Ctrl+C`) and run it again with the `--from-beginning` flag:
```bash
kafka-console-consumer.sh \
  --topic lab001.events \
  --bootstrap-server localhost:9092 \
  --from-beginning
```
You should now see all your previous messages!

## Step 5: Clean Up (Deleting Topics)

Once you are done experimenting, it is good practice to clean up your environment. Deleting a topic physically removes its data directories from the broker.

```bash
kafka-topics.sh \
  --delete \
  --topic lab001.events \
  --bootstrap-server localhost:9092
```

You can verify it has been removed by running the list command again:
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## 📝 Self-Assessment

To certify that you have completed this laboratory, answer the following questions.

<details>
<summary><b>1. Why is KRaft considered superior to Zookeeper for modern Kafka deployments?</b></summary>
<br>
Zookeeper is an external system, which meant administrators had to maintain, monitor, and secure two separate distributed systems (Kafka and ZK). KRaft removes this dependency by embedding the consensus algorithm directly into Kafka nodes acting as controllers. This greatly simplifies architecture, improves scalability, and reduces the time it takes for a cluster to elect new leaders.
</details>

<details>
<summary><b>2. If you create a topic with 5 partitions, but you only have 3 brokers, is this allowed? Why?</b></summary>
<br>
Yes, absolutely. Partitions are independent logs. A single broker can hold hundreds or thousands of partitions. Kafka will simply distribute the 5 partitions across the 3 brokers as evenly as possible.
</details>

<details>
<summary><b>3. What does it mean if a replica falls out of the ISR (In-Sync Replicas) list?</b></summary>
<br>
It means that a Follower broker has fallen too far behind the Leader (usually due to network lag, high GC pauses, or it crashed). If the Leader crashes while a replica is out of the ISR, that replica cannot be safely elected as the new Leader without risk of data loss.
</details>

<details>
<summary><b>4. Why can you increase the number of partitions for a topic, but you cannot decrease them?</b></summary>
<br>
Decreasing partitions is not allowed because it would orphan the data residing in the removed partitions. Kafka guarantees strictly increasing offsets for messages within a partition. If a partition was deleted, all messages in it would be lost, breaking consumer applications that rely on those offsets and causing silent data loss.
</details>

---
> [!TIP]
> You have successfully provisioned a cluster and manipulated its core structures natively. You can tear down the cluster by running `docker-compose down`. 
> 
> You are now ready to write Java code! Proceed to **LAB-002** to master the Producer API.
