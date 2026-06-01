# Concept: Kafka Streams API - Stateless Processing

## Kafka Streams vs Consumer/Producer API
While the standard Kafka Consumer and Producer APIs are excellent for moving bytes from point A to point B, they require you to manually manage state, handle threading, manage offsets during failures, and write boilerplate code for transformations.

**Kafka Streams** is a declarative Java library that abstracts these complexities. It allows you to define a stream processing **Topology** (a Directed Acyclic Graph of stream operations) and automatically handles scaling, fault tolerance, and threading.

## Stream Topology
A Topology consists of three types of nodes:
1. **Source Node**: Reads data from a Kafka topic (no predecessors).
2. **Processor Node**: Transforms data (e.g., Map, Filter).
3. **Sink Node**: Writes data to a Kafka topic (no successors).

## KStream
The `KStream` is the core abstraction in Kafka Streams representing a **record stream**. A record stream is an infinite sequence of independent, immutable records. It is often compared to an insert-only table in a database.

## Stateless Operations
In this lab, we focus on **stateless** operations. These are operations where processing a single record does not depend on any previously processed records.
- **`filter`**: Evaluates a boolean condition. If true, the record is passed downstream; if false, it is dropped.
- **`map`**: Transforms both the Key and the Value of a record. *Warning*: Changing the key will mark the record for repartitioning!
- **`mapValues`**: Transforms only the Value. The Key remains unchanged, avoiding costly repartitioning over the network.
- **`branch`**: Routes records to different downstream topologies based on predicates.

## Threading Model and Tasks
Kafka Streams scales by partitioning the input topic.
- A **Task** is the fundamental unit of parallelism. Kafka Streams creates one Task for each partition of the input topic.
- A **Stream Thread** (`num.stream.threads`) executes one or more Tasks. 
- If you have 6 partitions in your topic and 2 Stream Threads, each thread executes 3 Tasks. If a thread dies, its Tasks are reassigned to the remaining threads, ensuring fault tolerance.
