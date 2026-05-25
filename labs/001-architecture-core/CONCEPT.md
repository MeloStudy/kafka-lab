# Kafka Architecture: Core Concepts

Before we spin up a cluster and run commands, it is vital to understand what exactly we are provisioning. 

## 1. The Broker
A Kafka **Broker** is a single server (or container) running the Kafka process. A cluster is made up of multiple brokers. Brokers receive messages from producers, assign offsets to them, and commit the messages to storage on disk.

## 2. Topics, Partitions, and Offsets
- **Topic**: A logical name for a stream of records (e.g., `orders`, `user_signups`).
- **Partition**: A topic is broken down into one or more partitions. Partitions are the fundamental unit of parallelism in Kafka. If a topic has 3 partitions, it means 3 consumers can read from it simultaneously.
- **Offset**: Each message in a partition gets an incremental ID called an offset. Offsets are only meaningful *within a specific partition*. 

## 3. High Availability: Replicas and ISR
To ensure data is not lost if a broker crashes, Kafka duplicates partitions across multiple brokers. This is called the **Replication Factor (RF)**.
- If a topic has an RF of 3, every partition exists on 3 different brokers.
- **Leader**: One of the replicas is designated as the Leader. All reads and writes for that partition go exclusively to the Leader.
- **Followers**: The other replicas are Followers. They passively pull data from the Leader to keep up to date.
- **ISR (In-Sync Replicas)**: This is a critical concept. The ISR is the list of replicas that are fully caught up with the Leader. If a follower falls too far behind (due to network lag or crash), it is temporarily removed from the ISR list. If the Leader crashes, only a replica currently in the ISR can be elected as the new Leader.

## 4. The Consensus Problem: Zookeeper vs KRaft
For a cluster of brokers to function, they need a "brain" to manage metadata (e.g., "Broker 2 just crashed! We need to elect new Leaders for its partitions!").

- **Legacy (Zookeeper)**: Historically, Kafka relied on an external system called Apache Zookeeper. This meant managing two different distributed systems, which was complex and brittle at scale.
- **Modern (KRaft)**: Introduced via KIP-500, KRaft (Kafka Raft Metadata mode) completely removes Zookeeper. Kafka now manages its own metadata using the Raft consensus protocol. 
In KRaft mode, certain brokers are assigned the role of `controller`. The controllers form a quorum and elect an active controller among themselves to act as the cluster's brain. 

### ⚙️ How Configuration Changes (Zookeeper vs KRaft)
If you look at older Kafka tutorials, you will see environment variables pointing to Zookeeper. Here is how the configuration paradigm shifted:
- **No Zookeeper Dependency**: You no longer need to run a separate Zookeeper container.
- **Node Roles (`KAFKA_CFG_PROCESS_ROLES`)**: In KRaft, you must explicitly tell the node what role it plays: `broker` (stores data), `controller` (manages metadata), or both.
- **Node IDs (`KAFKA_CFG_NODE_ID`)**: Instead of `broker.id`, KRaft strictly uses `node.id`.
- **Quorum Voters (`KAFKA_CFG_CONTROLLER_QUORUM_VOTERS`)**: Instead of `zookeeper.connect`, brokers use this setting to know which nodes are the controllers that form the voting quorum (e.g., `0@kafka:9093`).

> [!NOTE]
> In this lab, we will deploy a **Single-Node KRaft Cluster**. This means our single Docker container will act as both a `broker` (storing data) and a `controller` (managing metadata). Since there is only one node, our Replication Factor is limited to 1.
