# P01: Enterprise Event-Driven Workflow - Core Concepts

## 1. Reactive Streams & Backpressure with Reactor Kafka
Project Reactor is a Reactive Streams implementation for Java. When combined with Kafka, it changes how the `poll()` loop behaves.
In standard Spring Kafka, a background thread continuously calls `poll()`. In `reactor-kafka`, the polling is driven by the demand (backpressure) signaled by the downstream subscribers.
This means if your downstream processing is slow, it signals less demand, and the `KafkaReceiver` reduces the polling frequency or pauses, preventing the application from being overwhelmed and avoiding out-of-memory errors.

## 2. Bridging REST to Kafka
A common challenge in event-driven microservices is bridging synchronous HTTP clients with asynchronous Kafka backends.
- **Bad Practice**: Blocking the HTTP thread until the Kafka consumer finishes processing.
- **Good Practice**: The REST endpoint receives the request, non-blockingly publishes an event to Kafka (e.g., `OrderCreatedEvent`), and immediately returns an `HTTP 202 Accepted` status. This informs the client that the request was received and will be processed eventually.

## 3. Eventual Consistency
Because the REST endpoint returns immediately, the system state is not updated instantly. The processing consumer (e.g., `OrderConsumer`) will read the event, process it, and emit a final result (`OrderConfirmedEvent` or `OrderFailedEvent`). The client must poll or rely on WebSockets/SSE to know the final outcome. This is the essence of eventual consistency.
