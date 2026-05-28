package com.kafka.lab;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class KafkaIntegrationTest {

    // 1. Define the Ephemeral Kafka Broker
    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"));

    private OrderProducer producer;
    private OrderConsumer consumer;

    @BeforeEach
    void setUp() {
        // 2. Inject the dynamic port/address into our application components
        String bootstrapServers = kafka.getBootstrapServers();
        
        producer = new OrderProducer(bootstrapServers);
        consumer = new OrderConsumer(bootstrapServers, "test-group");
    }

    @AfterEach
    void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    void shouldProduceAndConsumeOrder() {
        String topic = "orders.topic";
        String orderEvent = "ORDER-12345";

        // 3. Act: Produce a message
        producer.sendOrder(topic, orderEvent);

        // 4. Act: Consume the message
        String receivedOrder = consumer.pollOrder(topic, Duration.ofSeconds(5));

        // 5. Assert: Verify we got what we sent
        assertEquals(orderEvent, receivedOrder, "The consumed order should match the produced order.");
    }
}
