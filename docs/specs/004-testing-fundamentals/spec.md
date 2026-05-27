# Spec: LAB-004: Testing Fundamentals (TDD)
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Understand the Kafka testing pyramid (Unit Testing vs. Integration Testing).
- Understand why `EmbeddedKafka` is considered legacy and why **Testcontainers** is the modern industry standard.
- Establish a strict Test-Driven Development (TDD) baseline that all future Level 2+ labs will follow.
- Learn how to dynamically inject Kafka broker properties into application code during tests.

## 2. Core Concepts (CONCEPT.md)
*Theoretical concepts to be thoroughly explained:*
- **Integration Testing vs Mocks**: Why mocking Kafka clients (`MockProducer`/`MockConsumer`) often hides serialization and network serialization bugs.
- **Testcontainers Lifecycle**: How `@Testcontainers` and `@Container` manage the ephemeral Docker container lifecycle bound to the JUnit test execution.
- **The Shift from `docker-compose`**: Understanding that from this point forward in the syllabus, developers do not need to manually spin up `docker-compose up` to verify their logic.

## 3. Infrastructure & Tooling
- **Infrastructure Profile**: Ephemeral (Testcontainers). There will be **no** `docker-compose.yml` in this lab.
- **Libraries**: JUnit 5, `org.testcontainers:kafka`, `kafka-clients`, `slf4j-simple`.

## 4. Practical Implementation (README.md)
*Describe the hands-on part.*
- **Step 1: The Setup**: Configure the `pom.xml` with the necessary testing dependencies.
- **Step 2: Writing the Failing Test**: Create an integration test class (`KafkaIntegrationTest.java`). Spin up the Kafka container, write a test that produces a message and expects a consumer to receive it.
- **Step 3: The Implementation**: Write a simple `OrderProducer` and `OrderConsumer`.
- **Step 4: Making it Pass**: Run the test suite and watch Testcontainers automatically pull the Kafka image, start the broker, execute the test, and tear it down.

## 5. TDD & Technical Verification
- **TDD Status**: **Strict Enforcement**. This entire lab is dedicated to testing. The student will write the test suite before writing the actual Kafka components.

## 6. Resilience & Delivery Semantics
- **Testing Edge Cases**: We will discuss conceptually how Testcontainers allows us to test resilience features (e.g., stopping the container mid-test using Toxiproxy to simulate network partitions, though we won't implement Toxiproxy until Level 4).

## 7. Self-Assessment Questions
1. Why is Testcontainers generally preferred over `EmbeddedKafka` or `MockProducer` for production-grade applications?
2. How does the test code know which port the ephemeral Kafka broker is running on, given that Testcontainers uses random ports to avoid conflicts?
3. What is the downside of Integration Testing with Testcontainers compared to Unit Testing?
