package com.kafkalab.p01;

import com.kafkalab.p01.model.OrderRequest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Testcontainers
public class OrderWorkflowIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("kafka.producer.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("kafka.consumer.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private WebTestClient webTestClient;

    private static Consumer<String, String> testConsumer;

    @BeforeAll
    static void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "test-group", "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps, new StringDeserializer(), new StringDeserializer());
        
        testConsumer = cf.createConsumer();
        testConsumer.subscribe(Collections.singleton("orders.confirmed"));
    }

    @Test
    public void testOrderWorkflow_createsEventAndConfirms() {
        OrderRequest request = new OrderRequest("O-12345", "USER-99", 250.50);

        // 1. Send HTTP Request
        webTestClient.post()
                .uri("/api/orders")
                .bodyValue(request)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.status").isEqualTo("ACCEPTED")
                .jsonPath("$.orderId").isEqualTo("O-12345");

        // 2. Verify that an order event reached orders.confirmed (after consumer processes orders.inbound)
        ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(10));
        
        boolean found = false;
        for (ConsumerRecord<String, String> record : records) {
            if (record.value().contains("O-12345") && record.value().contains("CONFIRMED")) {
                found = true;
                break;
            }
        }
        
        assertThat(found).isTrue();
    }
}
