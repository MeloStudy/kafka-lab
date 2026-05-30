# Core Concepts: Reactive Kafka & Backpressure

## 1. The Reactive Streams Shift

In traditional Spring Kafka (`@KafkaListener`), processing is typically synchronous or thread-pool based. When a message is consumed via the `poll()` loop, the thread processes the message and then fetches more. If processing is slow, it blocks the thread, which can lead to starvation in a highly concurrent environment like an HTTP web server.

**Project Reactor** introduced the Reactive Streams specification (Publisher/Subscriber with non-blocking backpressure). `reactor-kafka` bridges Kafka's pull-based API with Reactor's push-based API.

## 2. Bridging REST to Kafka

In an Event-Driven architecture, REST APIs often act as the ingress point. To avoid blocking the Netty event loop threads (used by Spring WebFlux), `KafkaSender` allows us to publish events completely asynchronously.

```java
public Mono<Void> sendTransaction(Transaction transaction) {
    // ...
    return kafkaSender.send(Mono.just(senderRecord)).then();
}
```
This returns a `Mono<Void>` representing the completion of the Kafka publish action. The HTTP response is only sent back to the client once the Kafka Broker acknowledges the write, ensuring durability without blocking threads.

## 3. Reactive Consumption & Backpressure

The most complex part of reactive messaging is handling backpressure during consumption.

Kafka's native Java consumer uses a `poll(Duration)` loop. If your processing application (the consumer) is slower than the rate of incoming messages, you risk an `OutOfMemoryError` if you keep buffering, or a Consumer Rebalance if `max.poll.interval.ms` is exceeded.

`KafkaReceiver` in `reactor-kafka` handles this elegantly:
1. It requests a specific number of records based on the downstream subscriber's demand (`request(n)`).
2. It calls `poll()` and emits records to the Flux.
3. **Crucially**, if the downstream subscriber (e.g. your processing logic) applies backpressure (meaning it cannot keep up), `KafkaReceiver` uses Kafka's `consumer.pause()` API to temporarily stop fetching from the partitions. 
4. It continues to call `poll()` in the background to send heartbeats, preventing a rebalance, but fetches zero new records until the downstream signals more demand via `consumer.resume()`.

## 4. At-Least-Once Delivery & Manual Acks

By default, auto-commit is dangerous in reactive pipelines because messages might be fetched and "committed" by a background thread before the reactive pipeline has finished processing them asynchronously.

Instead, we disable auto-commit and use `ReceiverOffset.acknowledge()`:

```java
kafkaReceiver.receive()
    .concatMap(record -> processRecord(record)
        .doOnSuccess(v -> record.receiverOffset().acknowledge())
        .thenReturn(record)
    )
```

`concatMap` ensures that we process records sequentially (maintaining ordering per partition) and `acknowledge()` marks the offset as ready to be committed only AFTER the asynchronous `processRecord` Mono completes successfully. `reactor-kafka` periodically commits all acknowledged offsets.
