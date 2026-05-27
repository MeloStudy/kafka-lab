package com.melostudy.kafka.delivery;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ReadCommittedConsumer {

    private final KafkaConsumer<String, String> consumer;

    public ReadCommittedConsumer(String bootstrapServers, String groupId, String isolationLevel) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Critical for Exactly-Once Consumer
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel); // "read_committed" or "read_uncommitted"
        
        this.consumer = new KafkaConsumer<>(props);
    }

    public List<String> readMessages(String topic, int expectedCount) {
        consumer.subscribe(Collections.singletonList(topic));
        List<String> messages = new ArrayList<>();
        int retries = 0;
        
        while (messages.size() < expectedCount && retries < 15) {
            consumer.poll(Duration.ofMillis(500)).forEach(record -> messages.add(record.value()));
            retries++;
        }
        return messages;
    }

    public void close() {
        consumer.close();
    }
}
