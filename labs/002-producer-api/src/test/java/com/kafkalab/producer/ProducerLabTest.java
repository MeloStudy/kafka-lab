package com.kafkalab.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void testMockProducerKeyRouting() {
        // We use MockProducer with autoComplete=true to simulate immediate acks
        MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        // Send two messages with the same key
        ProducerRecord<String, String> record1 = new ProducerRecord<>("lab-topic", "user-123", "Event A");
        ProducerRecord<String, String> record2 = new ProducerRecord<>("lab-topic", "user-123", "Event B");
        
        mockProducer.send(record1);
        mockProducer.send(record2);

        // Verify they were both sent
        assertEquals(2, mockProducer.history().size());
        
        // With a MockProducer, we don't have the real DefaultPartitioner logic running to compute the partition integer,
        // but we can verify that the application correctly assigned the same key to both records, 
        // which guarantees they will be routed to the same partition by the real KafkaProducer.
        assertEquals(mockProducer.history().get(0).key(), mockProducer.history().get(1).key());
        assertEquals("user-123", mockProducer.history().get(0).key());
    }

    @Test
    public void testIdempotentProducerProperties() {
        Properties props = IdempotentProducer.buildProperties("localhost:9092");

        // Idempotence strictly requires certain configurations
        assertEquals("true", props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("all", props.getProperty(ProducerConfig.ACKS_CONFIG));
        
        // Retries must be > 0 (Integer.MAX_VALUE is used)
        assertTrue(Integer.parseInt(props.getProperty(ProducerConfig.RETRIES_CONFIG)) > 0);
        
        // Max in flight requests must be <= 5
        assertTrue(Integer.parseInt(props.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).toString()) <= 5);
    }
}
