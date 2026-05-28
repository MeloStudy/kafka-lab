package com.kafka.lab;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class VanillaConsumer {
    public static void main(String[] args) {
        String consumerId = UUID.randomUUID().toString().substring(0, 4);
        System.out.println("Starting Consumer: " + consumerId);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lab003-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // At-Least-Once Delivery Semantics is the default.
        // This is equivalent to setting enable.auto.commit = true explicitly.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        // The consumer will be kicked out of the group if it doesn't call poll() within 10 seconds
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000"); 

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Add a graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered. Waking up consumer...");
            // wakeup() interrupts consumer.poll() and throws WakeupException
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(Collections.singletonList("lab003.events"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[Consumer %s] Partition: %d, Offset: %d, Value: %s%n",
                            consumerId, record.partition(), record.offset(), record.value());

                    // ⚠️ LAB EXPERIMENT:
                    // Uncomment the line below to simulate a slow consumer that exceeds max.poll.interval.ms.
                    // This will trigger a rebalance and cause Kafka to revoke this consumer's partition.
                    // sleepFor(15000); 
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            System.out.println("WakeupException caught. Consumer is gracefully shutting down...");
        } finally {
            // Close the consumer to quickly trigger a rebalance instead of waiting for session timeout
            consumer.close();
            System.out.println("Consumer closed successfully.");
        }
    }

    private static void sleepFor(long ms) {
        try {
            System.out.println("Sleeping for " + ms + " ms to simulate slow processing...");
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
