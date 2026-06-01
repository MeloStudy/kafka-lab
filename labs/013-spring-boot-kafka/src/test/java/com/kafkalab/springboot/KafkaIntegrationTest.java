package com.kafkalab.springboot;

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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class KafkaIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"));

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private PaymentProducer producer;

    @Autowired
    private PaymentConsumer consumer;

    @BeforeEach
    void setup() {
        consumer.resetLatch();
    }

    @Test
    void testSendAndReceivePayment() throws InterruptedException {
        Payment payment = new Payment("P-123", "U-456", new BigDecimal("99.99"), "USD");

        producer.sendPayment(payment);

        boolean messageConsumed = consumer.getLatch().await(10, TimeUnit.SECONDS);

        assertThat(messageConsumed)
                .as("Message should be consumed within 10 seconds")
                .isTrue();

        assertThat(consumer.getLastReceivedPayment())
                .isNotNull()
                .satisfies(p -> {
                    assertThat(p.getId()).isEqualTo("P-123");
                    assertThat(p.getUserId()).isEqualTo("U-456");
                    assertThat(p.getAmount()).isEqualByComparingTo(new BigDecimal("99.99"));
                    assertThat(p.getCurrency()).isEqualTo("USD");
                });
    }
}
