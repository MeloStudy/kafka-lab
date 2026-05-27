# LAB-004: Testing Fundamentals (TDD)

In this lab, we adopt the modern standard for testing Kafka applications: **Testcontainers**. Notice that this directory does **not** contain a `docker-compose.yml`. We will not start any infrastructure manually!

## Step 1: Examine the Test Suite

Open `src/test/java/com/kafka/lab/KafkaIntegrationTest.java`. 

### 🔎 Code Dissection: Testcontainers
- `@Testcontainers`: Tells JUnit 5 to manage the lifecycle of Docker containers defined in this class.
- `@Container`: Marks the `KafkaContainer` instance. JUnit will start this container before any tests run, and stop it after all tests finish.
- `kafka.getBootstrapServers()`: This is the magic. Because the container binds to a random available port on your machine (to avoid conflicts), we must dynamically fetch the connection string and pass it to our `OrderProducer` and `OrderConsumer`.

## Step 2: Run the TDD Suite

We have already implemented the `OrderProducer` and `OrderConsumer` to make the test pass. 

Run the test suite using Maven:
```bash
mvn test
```

### What happens behind the scenes?
1. **Docker Boot**: You will see logs from Testcontainers (`🐳 [confluentinc/cp-kafka:7.4.0]`). It is pulling the image (if you don't have it) and starting a real Kafka broker inside a Docker container.
2. **Topic Creation**: The test creates a topic implicitly when the producer sends a message.
3. **Execution**: The `OrderProducer` sends `"ORDER-12345"`. The `OrderConsumer` polls and receives it.
4. **Assertion**: JUnit verifies the received order matches the sent order.
5. **Teardown**: The Docker container is immediately destroyed. 

If you see `BUILD SUCCESS`, congratulations! You have executed a production-grade Integration Test. 

> [!TIP]
> From now on, whenever we build complex topologies or reactive pipelines, we will write a Testcontainers suite first to verify the logic without ever touching a CLI tool.

---

## 📝 Self-Assessment

<details>
<summary><b>1. Why is Testcontainers generally preferred over `EmbeddedKafka` or `MockProducer` for production-grade applications?</b></summary>
<br>
Mocks do not test network serialization or true broker behavior. `EmbeddedKafka` is an in-memory simulation that lacks parity with real clusters (like KRaft or security configurations). Testcontainers runs the exact same Docker image that you would run in production, guaranteeing absolute fidelity.
</details>

<details>
<summary><b>2. How does the test code know which port the ephemeral Kafka broker is running on?</b></summary>
<br>
Testcontainers maps the internal container port to a random, ephemeral port on the host machine. We must call `kafka.getBootstrapServers()` at runtime to retrieve this dynamic connection string and inject it into our Kafka Clients.
</details>

<details>
<summary><b>3. What is the downside of Integration Testing with Testcontainers compared to Unit Testing?</b></summary>
<br>
Speed and Resource Consumption. Spinning up a Docker container takes several seconds and consumes significant RAM/CPU. A test suite with hundreds of Testcontainers tests can be very slow if containers are not shared properly across the test suite.
</details>
