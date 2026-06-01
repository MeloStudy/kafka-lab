# Concept: Spring Boot & Kafka

## Spring Kafka Architecture
While native `kafka-clients` require significant boilerplate for managing threads, polling loops, and committing offsets, **Spring Kafka** abstracts this away using high-level templates and declarative listeners.

Behind the scenes, Spring creates a `ConcurrentKafkaListenerContainerFactory`. This factory generates `MessageListenerContainer` instances that manage the threads and execute the `poll()` loop for you.

## KafkaTemplate
`KafkaTemplate` is a thread-safe, high-level abstraction for producing messages. It wraps the native `KafkaProducer` and provides convenient methods like `send(topic, key, value)`. It automatically handles serialization based on your Spring properties.

## @KafkaListener
The `@KafkaListener` annotation marks a method as the target for Kafka messages.
- **Concurrency**: By default, one listener thread is created. If you have a topic with 3 partitions, you can set `concurrency = "3"` in the annotation or properties. Spring will spawn 3 threads, each consuming from 1 partition concurrently.
- **Deserialization**: The byte payload is transparently converted into your Java POJO by the configured deserializer.

## Message Conversion
Instead of writing custom Byte serializers, Spring provides `JsonSerializer` and `JsonDeserializer` (backed by Jackson).
You define these in `application.yml`. When `KafkaTemplate.send()` is called with a POJO, it is serialized to JSON bytes. When the `@KafkaListener` receives bytes, they are deserialized back into a POJO.

## Resilience: ErrorHandlingDeserializer
If a producer sends invalid JSON (e.g., malformed syntax), the default `JsonDeserializer` will throw a `SerializationException`. 
Because the offset isn't committed when an exception is thrown, the consumer will infinitely loop, trying to process the exact same poisonous payload, effectively blocking the partition forever.
To solve this, Spring provides the `ErrorHandlingDeserializer`. It acts as a wrapper: if deserialization fails, it swallows the error, returns a null payload (or logs it), and allows the consumer to skip the bad message and commit the offset.
