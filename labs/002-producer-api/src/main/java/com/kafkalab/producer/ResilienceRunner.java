package com.kafkalab.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResilienceRunner {

    private static final Logger log = LoggerFactory.getLogger(ResilienceRunner.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting Idempotent Resilience Runner. Press Ctrl+C to stop.");
        
        try (KafkaProducer<String, String> producer = IdempotentProducer.createProducer("localhost:9092")) {
            long i = 0;
            while (true) {
                String payload = "Resilience test message " + i;
                producer.send(new ProducerRecord<>("producer-lab", "resilience-key", payload), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send message: {}", exception.getMessage());
                    } else {
                        log.info("Successfully sent message to partition {} at offset {}", metadata.partition(), metadata.offset());
                    }
                });
                
                i++;
                Thread.sleep(1000); // Send 1 message per second
            }
        }
    }
}
