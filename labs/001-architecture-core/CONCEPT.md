# Kafka Architecture: Core Concepts

Before we spin up a cluster and run commands, it is vital to understand the physical and logical reality of what we are provisioning. This is not a superficial overview; we will look at how Kafka operates at the system level.

## 1. The Broker (The Physical Node)
A Kafka **Broker** is a single JVM process running on a server (or container). A cluster is made up of multiple brokers acting together.
- **System Resources**: While Kafka runs on the JVM, it intentionally keeps its Heap Memory small (usually 1-2GB). It relies heavily on the **OS Page Cache** (system RAM) to buffer reads and writes, bypassing the JVM Garbage Collector.
- **Disk I/O**: The broker's primary job is receiving bytes from the network and appending them directly to disk. It uses a zero-copy mechanism (`sendfile` system call) to transfer data directly from the OS Page Cache to the Network Socket, making it incredibly fast.

## 2. Topics, Partitions, and Offsets
- **Topic**: A logical grouping name for a stream of records (e.g., `orders`). Topics do not physically exist on disk; they are metadata.
- **Partition**: The physical manifestation of a topic.
  - **The Physical View**: Physically, a partition is a directory on the Broker's hard drive (e.g., `orders-0`). Inside this directory, Kafka writes the events to append-only log files (segments like `0000.log`). 
  - **Resource Cost**: Partitions consume OS File Descriptors and OS Page Cache. Creating hundreds of thousands of partitions on a small cluster will cause high latency due to massive metadata overhead and disk thrashing.
  > 📐 **Architecture Note (Partition Limits)**: A general rule of thumb is to limit partitions to ~4,000 per broker without KRaft. With KRaft, modern clusters can handle hundreds of thousands of partitions per cluster, but you should still avoid unnecessary over-partitioning.
- **Offset**: An immutable, strictly increasing 64-bit integer assigned to every message within a partition.
  - **Index Files**: To quickly find a message by its offset, Kafka maintains an index file (`0000.index`) mapping the offset to the physical byte position in the `.log` file.
  - **Immutability**: Once an offset is assigned and written to disk, it can NEVER be changed or deleted until the entire segment expires.

### ⚠️ Modifying and Deleting Topics
- **Increasing Partitions**: You can dynamically increase the number of partitions for a topic. The new partitions will be created as empty directories on the brokers.
- **Decreasing Partitions**: You **CANNOT** decrease the number of partitions. Doing so would orphan the data residing in the removed partitions, breaking the strictly increasing offset guarantee and causing data loss.
- **Deleting Topics**: Deleting a topic physically removes all associated partition directories and `.log` files from the brokers' hard drives. This is irreversible.

## 3. High Availability: Replicas and ISR
To prevent data loss during hardware failures, Kafka uses a **Replication Factor (RF)** to copy partitions across different brokers.
> ⚠️ **Golden Rule**: The Replication Factor **CANNOT** exceed the total number of brokers in your cluster. If you have 3 brokers, the maximum RF is 3. Attempting to set an RF of 4 will result in an immediate CLI error.

- **Leader**: Exactly one broker is elected as the Leader for a partition. ALL read and write requests from clients must go to this Leader.
- **Followers (Replica Fetchers)**: The other brokers holding copies are Followers. They do not serve clients. Instead, they run background threads that constantly pull (fetch) new data from the Leader.
- **The High Watermark**: A message is only considered "Committed" (safe for consumers to read) when it has been successfully replicated to all in-sync followers. This committed offset boundary is called the High Watermark.
- **ISR (In-Sync Replicas)**: This is a dynamic list of followers that are fully caught up with the Leader. 
  - If a follower crashes or its network lags beyond `replica.lag.time.max.ms`, the Leader kicks it out of the ISR.
  - If the Leader crashes, the controller can ONLY promote a follower that is currently inside the ISR to be the new Leader. Promoting an out-of-sync replica would result in permanent data loss.

## 4. The Consensus Problem: Zookeeper vs KRaft
A distributed cluster needs a central "brain" to track metadata: Which brokers are alive? Who is the leader of partition `orders-0`? 

- **Legacy (Zookeeper)**: Historically, Kafka relied on Apache Zookeeper for this. This required running a separate, fragile distributed system just to keep Kafka running.
- **Modern (KRaft)**: Introduced via KIP-500, Kafka now manages its own metadata using the Raft consensus protocol.
  - **The Metadata Log**: Instead of storing cluster state in Zookeeper, KRaft stores the cluster metadata as a standard Kafka topic (`__cluster_metadata`). 
  - **Quorum Controllers**: Certain brokers are designated with the `controller` role. They form a voting quorum. One is elected the Active Controller. When a broker crashes, the Active Controller appends a "Broker Down" event to the metadata log, and all other brokers instantly read that event and adjust their routing tables.
  > 📐 **Architecture Note (Controller Quorum Sizing)**: Production clusters should use an **odd number of controllers** (typically 3). Since KRaft uses majority voting, a 3-controller quorum can tolerate 1 failure, and a 5-controller quorum can tolerate 2. Having an even number provides no extra fault tolerance but slows down voting.

### ⚙️ How Configuration Changes (Zookeeper vs KRaft)
If you look at older Kafka tutorials, you will see environment variables pointing to Zookeeper. Here is the modern paradigm:
- **Node Roles (`KAFKA_CFG_PROCESS_ROLES`)**: In KRaft, you must explicitly declare if a node is a `broker` (stores user data), a `controller` (votes on metadata), or both.
- **Quorum Voters (`KAFKA_CFG_CONTROLLER_QUORUM_VOTERS`)**: Brokers use this setting to know the addresses of the controllers (e.g., `1@kafka:9093`).

> [!NOTE]
> In this lab, we deploy a **Single-Node KRaft Cluster**. This means our single Docker container will act as both a `broker` and a `controller`. Our Replication Factor will be 1, meaning the ISR list will only ever contain this single node.
