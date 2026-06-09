# CONCEPT: Observability, Tracing & Chaos

## The Three Pillars of Observability in Event-Driven Systems

When moving from a synchronous REST architecture to an asynchronous Event-Driven Architecture (EDA) with Kafka, observability becomes significantly more complex. A single user action might trigger events across ten different microservices.

1.  **Metrics (JMX & Micrometer)**: Provide aggregate views of system health over time (e.g., Consumer Lag, Throughput, Error Rates). Kafka natively exposes hundreds of metrics via Java Management Extensions (JMX). Micrometer acts as a facade, collecting these JMX metrics and exposing them in formats Prometheus can scrape.
2.  **Logs**: Provide granular, unstructured data about discrete events.
3.  **Distributed Tracing (OpenTelemetry)**: Connects logs and metrics across service boundaries to form a single "Trace".

## Deep Dive: How Tracing Context Propagates through Kafka

In a synchronous REST call, HTTP headers (`traceparent`) are passed from Client to Server. In Kafka, there is no direct network connection between the Producer and the Consumer.

### The W3C Trace Context
The industry standard is the W3C Trace Context, consisting of:
-   **Trace ID**: A globally unique identifier for the entire transaction (e.g., `4bf92f3577b34da6a3ce929d0e0e4736`).
-   **Span ID**: A unique identifier for a specific operation within the trace (e.g., `00f067aa0ba902b7`).

### Kafka Headers as the Transport Layer
Kafka records consist of a Key, a Value, a Timestamp, and **Headers**. 
When a Producer (instrumented with OpenTelemetry) sends a message:
1.  It creates a "Producer Span".
2.  It injects the Trace ID and the Producer Span ID into the Kafka Record Headers (specifically a header named `traceparent`).
3.  The Kafka Broker stores the message completely agnostic to the headers.
4.  When the Consumer polls the message, the OpenTelemetry instrumentation extracts the `traceparent` header.
5.  It creates a new "Consumer Span" that explicitly links to the Producer Span as its parent, thus bridging the asynchronous gap.

## Chaos Engineering with Toxiproxy

Distributed systems are prone to network failures. Chaos Engineering is the practice of intentionally injecting failures into a system to verify its resilience.

**Toxiproxy** is a TCP proxy developed by Shopify. Instead of the Spring Boot application connecting directly to the Kafka Broker on port 9092, it connects to Toxiproxy. Toxiproxy then forwards the traffic to Kafka.

### The Consumer Poll Loop under Network Duress
Kafka consumers operate using a background heartbeat thread and a foreground `poll()` loop.
If Toxiproxy cuts the connection or injects massive latency (e.g., > `max.poll.interval.ms`):
1.  The background heartbeat thread fails to reach the Group Coordinator.
2.  The Coordinator marks the consumer as dead and triggers a **Rebalance**.
3.  When the network recovers, the consumer must rejoin the group and receive a new partition assignment.
4.  Because we use **At-Least-Once** delivery semantics (default), any messages consumed but not committed before the network partition will be redelivered, potentially resulting in duplicates.
