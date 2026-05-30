# Lab 014: Reactive Kafka (WebFlux Integration)

Welcome to the Reactive Kafka lab! In this module, we will explore how to integrate Apache Kafka with Project Reactor and Spring WebFlux. 

## Objectives
- Integrate `reactor-kafka` with a Spring Boot WebFlux application.
- Observe non-blocking backpressure mechanisms.
- Manage manual offset acknowledgement for At-Least-Once delivery.

## 1. Interactive Self-Assessment

<details>
<summary>How does <code>reactor-kafka</code> handle backpressure when the consumer is slow?</summary>
<br>
It translates the Reactor <code>request(n)</code> signal into Kafka's <code>consumer.pause()</code> and <code>consumer.resume()</code> APIs. If the downstream is slow, it pauses the partitions so that the internal <code>poll()</code> loop can still send heartbeats without fetching more data into memory.
</details>

<details>
<summary>Why should we disable <code>enable.auto.commit</code> in a reactive pipeline?</summary>
<br>
Because reactive processing happens asynchronously on different threads. An auto-commit might commit the offset of a message that was fetched but hasn't actually finished processing in the reactive chain yet, leading to message loss on failure.
</details>

## 2. Infrastructure Dissection

This lab uses **Testcontainers** for automated infrastructure. 
Instead of spinning up a `docker-compose.yml` manually, Spring Boot automatically detects the `@Testcontainers` annotation and spins up a KRaft broker programmatically for the integration test.

```java
@Container
static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"));
```
* **KRaft Mode**: Testcontainers now supports running Kafka in KRaft mode natively without Zookeeper.
* **Dynamic Property Binding**: `kafka.getBootstrapServers()` injects the random mapped port into the Spring context.

## 3. Running the Integration Test

Let's execute the TDD integration test which proves the entire pipeline works end-to-end.

```bash
# Command Dissection:
# mvn          : Invokes the Maven build tool
# -pl          : Project List (specifies which module to run)
# labs/014-reactive-kafka : The specific module
# clean test   : Cleans the target directory and runs the test phase
mvn -pl labs/014-reactive-kafka clean test
```

## 4. Observing the Application (Optional)

If you were to run the Spring Boot application against a local cluster, you could observe the manual offset commits using the CLI:

```bash
# Command Dissection:
# kafka-consumer-groups.sh : CLI tool to manage and inspect consumer groups
# --bootstrap-server       : The address of the Kafka broker
# --describe               : Action to describe a group's state
# --group                  : The specific consumer group ID configured in KafkaConfig.java
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group reactive-group
```
You would see the `CURRENT-OFFSET` slowly increase as the `ReactiveConsumer` processes each message with the artificial 500ms delay, demonstrating backpressure in action!
