# Spec: LAB-015: Observability, Tracing & Chaos
**Status**: `[READY]`

## 1. Pedagogical Objectives
- Understand the importance of observability in distributed event-driven systems.
- Instrument Kafka clients and Spring Boot applications with Micrometer for metric collection.
- Implement distributed tracing using OpenTelemetry to track events across producer and consumer boundaries.
- Introduce Chaos Engineering concepts by simulating network partitions using Toxiproxy to observe system behavior and recovery.

## 2. Core Concepts (CONCEPT.md)
- Concept 1: **Observability Pillars**: Metrics, Logs, and Traces in the context of Kafka.
- Concept 2: **JMX Metrics & Micrometer**: Exposing Kafka consumer/producer metrics (lag, throughput, error rates).
- Concept 3: **Distributed Tracing**: Context propagation via Kafka Headers, Span IDs, and Trace IDs (OpenTelemetry/Zipkin/Jaeger).
- Concept 4: **Chaos Engineering**: Simulating failures (latency, network partitions, broker crashes) and understanding their impact on producer acknowledgments and consumer group rebalances.

## 3. Infrastructure & Tooling
- Infrastructure Profile: **Option A: Basic KRaft Broker** + **Prometheus/Grafana/Zipkin** + **Toxiproxy**
  - We will use a customized `docker-compose.yml` that includes Kafka, observability tools, and Toxiproxy.
- CLI Commands:
  - `toxiproxy-cli create ...` to set up network proxies.
  - `toxiproxy-cli toxic add ...` to inject latency or cut connections.

## 4. Practical Implementation (README.md)
- Step 1: Bootstrap a Spring Boot application with `spring-boot-starter-actuator`, `micrometer-registry-prometheus`, and `micrometer-tracing-bridge-otel`.
- Step 2: Configure a Producer and Consumer to propagate tracing headers.
- Step 3: Set up the Docker infrastructure (Kafka, Prometheus, Zipkin, Toxiproxy).
- Step 4: Run the application and observe metrics in Prometheus and traces in Zipkin.
- Step 5: Inject a network partition between the Consumer and Kafka using Toxiproxy. Observe the consumer group rebalance and recovery once the partition is resolved.

## 5. TDD & Technical Verification
- Test 1: Verify that tracing headers (e.g., `traceparent`) are correctly injected into Kafka records by the Producer using `Testcontainers`.
- Test 2: Verify that the Consumer extracts the tracing headers and continues the span.
- Test 3: Verify the application's resilience by writing a test using Toxiproxy Testcontainers module to assert that the application recovers from a simulated network outage.

## 6. Resilience & Delivery Semantics
- **Delivery Semantics**: At-Least-Once. We will observe how network partitions affect offset commits and message redelivery.
- **Backpressure/Polling**: We will observe the impact of high network latency (injected by Toxiproxy) on the consumer `poll()` loop, potentially causing `max.poll.interval.ms` violations and triggering rebalances.

## 7. Self-Assessment Questions
1. How does context propagation work across asynchronous Kafka boundaries?
2. Which Kafka headers are used by OpenTelemetry to pass tracing information?
3. What happens to a Consumer Group if a network partition prevents heartbeats from reaching the Group Coordinator?
