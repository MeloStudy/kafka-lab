# Spec: LAB-009: Advanced Error Handling & DLTs
**Status**: `[DRAFT]`

## 1. Pedagogical Objectives
- Understand the difference between transient and fatal errors in message processing.
- Implement non-blocking retries using Spring Kafka's `@RetryableTopic`.
- Configure and manage Dead Letter Topics (DLT) for messages that exhaust retry attempts.
- Learn to use `DefaultErrorHandler` for granular exception classification and error handling strategies.

## 2. Core Concepts (CONCEPT.md)
- **Exception Classification**: Distinguishing between Transient (e.g., network timeout, temporary database unavailability) and Fatal exceptions (e.g., deserialization failure, invalid data format).
- **Dead Letter Topics (DLT)**: Concept of routing poisoned or continuously failing messages to a separate topic for manual inspection or alternate processing, preventing head-of-line blocking.
- **Non-blocking Retries**: Mechanics of Spring Kafka's `@RetryableTopic`, which uses separate topics for each retry attempt with backoff, freeing the consumer thread to process subsequent messages.
- **DefaultErrorHandler**: Spring Kafka's mechanism to handle listener exceptions, configure backoffs (fixed, exponential), and add recoverers.

## 3. Infrastructure & Tooling
- **Infrastructure Profile**: Testcontainers (Service Connections) - No `docker-compose.yml` needed for local development, as Spring Boot will manage the Kafka broker via Testcontainers.
- **CLI Commands**: `kafka-topics.sh` to inspect the generated retry and DLT topics. `kafka-console-consumer.sh` to read from the DLT.

## 4. Practical Implementation (README.md)
- **Step 1**: Scaffold a Spring Boot application with Spring Kafka and Testcontainers.
- **Step 2**: Create a Kafka producer to send simulated event payloads (e.g., `PaymentEvent`) utilizing explicitly configured JSON serialization (`JsonSerializer` for the producer and `JsonDeserializer` for the consumer).
- **Step 3**: Implement a basic `@KafkaListener` that intentionally throws exceptions based on the payload to simulate failures (e.g., transient network error vs invalid payload).
- **Step 4**: Configure `@RetryableTopic` with backoff settings (e.g., 3 attempts, exponential backoff) and specify fatal exceptions that bypass retries and go straight to DLT.
- **Step 5**: Implement a DLT listener using `@DltHandler` to log or process the failed messages.
- **Step 6**: Configure `DefaultErrorHandler` globally as an alternative or complement to annotations.

## 5. TDD & Technical Verification
- **Test 1**: `PaymentEventConsumerTest` - Verify that a transient exception triggers a retry (assert the message is sent to the retry topic and eventually processed if the simulated error resolves).
- **Test 2**: `PaymentEventConsumerDltTest` - Verify that after exhausting retries, the message is routed to the DLT (assert the DLT listener receives the exact message payload).
- **Test 3**: `FatalExceptionTest` - Verify that a fatal exception (e.g., `IllegalArgumentException`) immediately routes the message to the DLT without intermediate retries.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: At-Least-Once processing. We must ensure messages are not acknowledged (offset committed) until successfully processed or explicitly routed to a DLT.
- **Backpressure / Flow Control**: Non-blocking retries demonstrate how to avoid head-of-line blocking. The consumer's `poll()` loop remains active, processing new messages while failing messages are delayed in separate retry topics, preventing consumer starvation and partition lag.

## 7. Self-Assessment Questions
1. Why might you choose non-blocking retries (`@RetryableTopic`) over blocking retries within the same `poll()` loop?
2. What is the difference between a transient error and a fatal error in the context of stream processing?
3. What happens to the offset of a message that fails all retry attempts and is sent to a DLT?
