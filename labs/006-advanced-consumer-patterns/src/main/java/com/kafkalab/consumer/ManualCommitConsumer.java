package com.kafkalab.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ManualCommitConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ManualCommitConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ManualCommitConsumer(Properties props, String topic) {
        // Enforce manual commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                if (records.isEmpty()) {
                    continue;
                }

                // Process records
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Processed: key={}, value={}, offset={}", record.key(), record.value(), record.offset());
                    // Simulate an exception if the value is "poison_pill"
                    if ("poison_pill".equals(record.value())) {
                        throw new RuntimeException("Simulated processing failure!");
                    }
                }

                // Asynchronous commit for higher throughput
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        log.error("Async commit failed for offsets {}", offsets, exception);
                    } else {
                        log.debug("Async commit successful for offsets {}", offsets);
                    }
                });
            }
        } catch (CommitFailedException e) {
            log.error("Commit failed due to rebalance", e);
        } catch (RuntimeException e) {
            log.error("Error processing records", e);
            throw e; // Rethrow to let tests catch it
        } finally {
            log.info("Consumer is shutting down. Performing synchronous commit.");
            try {
                // Ensure offsets are committed before shutting down completely
                consumer.commitSync();
            } catch (Exception e) {
                log.error("Failed to perform final sync commit", e);
            } finally {
                consumer.close();
            }
        }
    }

    public void stop() {
        running.set(false);
    }
}
