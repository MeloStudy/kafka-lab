# Lab 009: Advanced Error Handling & DLTs

This lab demonstrates how to implement non-blocking retries and Dead Letter Topics (DLT) using Spring Kafka's `@RetryableTopic`.

## Objective
Build a robust consumer that differentiates between transient and fatal errors, utilizing retries efficiently without blocking the main partition.

## Hands-On Implementation

### 1. Explore the Code
- Open `PaymentConsumer.java`.
- Observe the `@RetryableTopic` annotation. It dictates that messages will be retried 3 times with an exponential backoff.
- Notice the `exclude = {IllegalArgumentException.class}`. This classifies the exception as **Fatal**.

### 2. Run the Application
Start the Spring Boot application. Because we are using **Testcontainers**, you do not need to run `docker-compose`. Spring Boot will automatically spin up a Kafka container for you.
```bash
mvn spring-boot:run
```

### 3. CLI Commands & Verification

While the application is running, use the Kafka CLI (if you have local tools installed) or simply observe the application logs to see how the topics were generated.

**Command Dissection: Listing Topics**
```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \ # The address of the broker
  --list                            # Command to list all topics
```
*You will see the main topic `payments`, along with `payments-retry-0`, `payments-retry-1`, `payments-retry-2`, and `payments-dlt` auto-created by Spring Kafka.*

### 4. Triggering Errors

Using a producer script or REST endpoint (in our tests we use `PaymentConsumerIntegrationTest.java`), we send events with specific statuses.
- Status `TRANSIENT_ERROR` throws a `RuntimeException`. You will see it retried across the `-retry` topics before landing in the DLT.
- Status `FATAL_ERROR` throws an `IllegalArgumentException`. You will see it land **immediately** in the DLT.

## Self-Assessment

<details>
<summary>1. Why might you choose non-blocking retries (`@RetryableTopic`) over blocking retries within the same `poll()` loop?</summary>
Blocking retries halt the consumer thread, preventing the processing of subsequent healthy messages in the partition (Head-of-Line blocking). Non-blocking retries forward the failing message to a separate topic, allowing the main loop to continue.
</details>

<details>
<summary>2. What is the difference between a transient error and a fatal error in the context of stream processing?</summary>
A transient error (like a network timeout) might resolve itself if retried. A fatal error (like a malformed JSON payload) will never succeed regardless of how many times it is retried.
</details>

<details>
<summary>3. What happens to the offset of a message that fails all retry attempts and is sent to a DLT?</summary>
The offset in the original (or retry) topic is committed. As far as that specific topic is concerned, the message is "processed". The responsibility of the message is now transferred to the DLT.
</details>
