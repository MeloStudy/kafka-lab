package com.kafkalab.reactive;

import com.kafkalab.reactive.consumer.ReactiveConsumer;
import com.kafkalab.reactive.model.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReactiveKafkaIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"));

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        ReactiveConsumer.MESSAGE_COUNT.set(0);
    }

    @Test
    void testTransactionProcessingFlow() throws InterruptedException {
        Transaction transaction = new Transaction("tx-100", "acc-42", new BigDecimal("150.00"));

        webTestClient.post()
                .uri("/api/transactions")
                .bodyValue(transaction)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(String.class)
                .value(response -> assertThat(response).contains("tx-100"));

        // Wait for consumer to process message
        int retries = 0;
        while (ReactiveConsumer.MESSAGE_COUNT.get() < 1 && retries < 20) {
            Thread.sleep(500);
            retries++;
        }
        
        assertThat(ReactiveConsumer.MESSAGE_COUNT.get()).isGreaterThanOrEqualTo(1);
    }
}
