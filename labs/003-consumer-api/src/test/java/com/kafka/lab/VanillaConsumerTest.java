package com.kafka.lab;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VanillaConsumerTest {

    @Test
    void testConsumerPollsMessages() {
        // Create a MockConsumer with an "earliest" offset reset strategy
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        
        // Define topic and partition
        String topic = "lab003.events";
        TopicPartition partition = new TopicPartition(topic, 0);

        // Assign the partition to the mock consumer
        mockConsumer.assign(Collections.singletonList(partition));

        // Set the starting offset
        Map<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(partition, 0L);
        mockConsumer.updateBeginningOffsets(startOffsets);

        // Add a dummy record to the mock consumer
        mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key1", "value1"));
        mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 1L, "key2", "value2"));

        // Poll messages from the mock consumer
        var records = mockConsumer.poll(Duration.ofMillis(100));

        // Assert that the consumer correctly polled 2 messages
        assertEquals(2, records.count(), "Consumer should poll exactly 2 records");
        
        var iter = records.iterator();
        assertEquals("value1", iter.next().value(), "First message should be value1");
        assertEquals("value2", iter.next().value(), "Second message should be value2");
        
        mockConsumer.close();
    }
}
