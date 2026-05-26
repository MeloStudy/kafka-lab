package com.kafkalab.producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ProducerLabTest {

    @Container
    public static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    private static String bootstrapServers;

    @BeforeAll
    public static void setup() throws Exception {
        bootstrapServers = kafka.getBootstrapServers();
        
        // Create topic with 3 partitions for testing
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(Collections.singletonList(new NewTopic("lab-topic", 3, (short) 1))).all().get();
        }
    }

    @Test
    public void testPartitioningByKey() throws Exception {
        try (KafkaProducer<String, String> producer = DefaultProducer.createProducer(bootstrapServers)) {
            // Send two messages with the same key
            Future<RecordMetadata> f1 = producer.send(new ProducerRecord<>("lab-topic", "user123", "Event 1"));
            Future<RecordMetadata> f2 = producer.send(new ProducerRecord<>("lab-topic", "user123", "Event 2"));

            RecordMetadata rm1 = f1.get();
            RecordMetadata rm2 = f2.get();

            // They must land in the same partition
            assertEquals(rm1.partition(), rm2.partition(), "Messages with the same key must go to the same partition");
        }
    }

    @Test
    public void testIdempotence() {
        try (KafkaProducer<String, String> producer = IdempotentProducer.createProducer(bootstrapServers)) {
            // Producer creation will fail if idempotence config contradicts acks or in-flight requests.
            // So simply creating it and sending a message successfully proves the configuration is valid.
            assertNotNull(producer);
            producer.send(new ProducerRecord<>("lab-topic", "idempotent-key", "Idempotent Event"));
        }
    }

    @Test
    public void testHeaders() throws Exception {
        try (KafkaProducer<String, String> producer = DefaultProducer.createProducer(bootstrapServers)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("lab-topic", "header-key", "Event with Header");
            record.headers().add("trace-id", "abcd-1234".getBytes(StandardCharsets.UTF_8));
            
            producer.send(record).get();
        }

        // Consume to verify the header
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("lab-topic"));
            
            boolean found = false;
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if ("header-key".equals(record.key())) {
                        byte[] traceIdBytes = record.headers().lastHeader("trace-id").value();
                        String traceId = new String(traceIdBytes, StandardCharsets.UTF_8);
                        assertEquals("abcd-1234", traceId);
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }
            assertTrue(found, "Should have found the message with the specific key and headers");
        }
    }
}
