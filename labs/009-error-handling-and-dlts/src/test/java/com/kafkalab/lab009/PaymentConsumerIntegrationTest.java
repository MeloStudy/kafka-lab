package com.kafkalab.lab009;

import com.kafkalab.lab009.consumer.PaymentConsumer;
import com.kafkalab.lab009.model.PaymentEvent;
import com.kafkalab.lab009.producer.PaymentProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
public class PaymentConsumerIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private PaymentProducer producer;

    @Autowired
    private PaymentConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer.processedEvents.clear();
        consumer.dltEvents.clear();
    }

    @Test
    void testSuccessfulPayment() {
        PaymentEvent event = new PaymentEvent("1", new BigDecimal("100.0"), "SUCCESS");
        producer.sendPayment(event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertEquals(1, consumer.processedEvents.size());
            assertEquals("1", consumer.processedEvents.get(0).getId());
            assertTrue(consumer.dltEvents.isEmpty());
        });
    }

    @Test
    void testTransientErrorGoesToDltAfterRetries() {
        // Will fail 3 times and then go to DLT
        PaymentEvent event = new PaymentEvent("2", new BigDecimal("200.0"), "TRANSIENT_ERROR");
        producer.sendPayment(event);

        // Given backoff of 1s, 2s, 4s, it might take ~7 seconds minimum to reach DLT.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertTrue(consumer.processedEvents.isEmpty());
            assertEquals(1, consumer.dltEvents.size());
            assertEquals("2", consumer.dltEvents.get(0).getId());
        });
    }

    @Test
    void testFatalErrorGoesDirectlyToDlt() {
        // IllegalArgumentException is excluded from retries, should route immediately
        PaymentEvent event = new PaymentEvent("3", new BigDecimal("300.0"), "FATAL_ERROR");
        producer.sendPayment(event);

        // Should be almost immediate since there are no retries
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertTrue(consumer.processedEvents.isEmpty());
            assertEquals(1, consumer.dltEvents.size());
            assertEquals("3", consumer.dltEvents.get(0).getId());
        });
    }
}
