package com.kafkalab.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class BackpressureRunner {
    private static final Logger log = LoggerFactory.getLogger(BackpressureRunner.class);

    public static void main(String[] args) {
        log.info("Starting Backpressure Runner to simulate BufferExhaustedException...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Intentionally create a bottleneck
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024); // Only 1KB of total buffer memory
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);  // Only wait for 1 second when buffer is full
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 512); // Batch size takes up half the buffer

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            log.info("Sending messages rapidly to exhaust the buffer...");
            // We loop very fast to fill up the 1KB buffer
            for (int i = 0; i < 1000; i++) {
                String payload = "This is a relatively large payload to fill the buffer faster. Message: " + i;
                producer.send(new ProducerRecord<>("producer-lab", "key", payload), (metadata, exception) -> {
                    if (exception != null) {
                        // In an async callback, this usually means the network threw an error or delivery timeout
                        log.error("Async send failed for message {}", exception.getMessage());
                    }
                });
                log.info("Queued message {}", i);
            }
        } catch (TimeoutException e) {
            log.error("Caught expected exception due to backpressure: {}", e.getClass().getSimpleName());
            log.error("Message: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Caught unexpected exception: {}", e.getMessage(), e);
        }
        
        log.info("Backpressure simulation completed.");
    }
}
