package com.kafkalab.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ProducerLabTest {

    @Test
    public void testMockProducerHeadersAndKey() {
        // MockProducer allows us to unit test our logic without a real broker
        // Integration Testing with real brokers is officially introduced in LAB-004.
        MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        ProducerRecord<String, String> record = new ProducerRecord<>("lab-topic", "my-key", "Event Data");
        record.headers().add("trace-id", "abcd-1234".getBytes(StandardCharsets.UTF_8));
        
        mockProducer.send(record);

        // Verify the message was sent correctly in memory
        assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, String> sentRecord = mockProducer.history().get(0);
        
        assertEquals("my-key", sentRecord.key());
        assertNotNull(sentRecord.headers().lastHeader("trace-id"));
        assertEquals("abcd-1234", new String(sentRecord.headers().lastHeader("trace-id").value(), StandardCharsets.UTF_8));
    }
}
