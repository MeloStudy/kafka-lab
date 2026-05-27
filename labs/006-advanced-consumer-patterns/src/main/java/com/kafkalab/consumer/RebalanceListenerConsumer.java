package com.kafkalab.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RebalanceListenerConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RebalanceListenerConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger revokedCount = new AtomicInteger(0);
    private final AtomicInteger assignedCount = new AtomicInteger(0);

    public RebalanceListenerConsumer(Properties props, String topic) {
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(props);
        
        // Subscribe with Rebalance Listener
        this.consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("Partitions revoked: {}", partitions);
                revokedCount.incrementAndGet();
                try {
                    // Synchronously commit any pending offsets before partitions are taken away
                    consumer.commitSync();
                    log.info("Committed offsets on partition revocation.");
                } catch (Exception e) {
                    log.error("Failed to commit offsets on revocation", e);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("Partitions assigned: {}", partitions);
                assignedCount.incrementAndGet();
            }
        });
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                if (records.isEmpty()) {
                    continue;
                }

                records.forEach(record -> log.info("Processed: value={}", record.value()));
                
                consumer.commitAsync();
            }
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    public void stop() {
        running.set(false);
    }

    public int getRevokedCount() {
        return revokedCount.get();
    }

    public int getAssignedCount() {
        return assignedCount.get();
    }
}
