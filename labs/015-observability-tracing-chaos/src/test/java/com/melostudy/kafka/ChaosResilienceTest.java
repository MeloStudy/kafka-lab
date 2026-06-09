package com.melostudy.kafka;

import com.melostudy.kafka.consumer.OrderConsumer;
import com.melostudy.kafka.producer.OrderProducer;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class ChaosResilienceTest {

    private static final Network network = Network.newNetwork();

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka");

    @Container
    private static final ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network)
            .dependsOn(kafka);

    private static ToxiproxyContainer.ContainerProxy proxy;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        proxy = toxiproxy.getProxy(kafka, 9092);
        // Point Spring Boot to Toxiproxy instead of Kafka directly
        registry.add("spring.kafka.bootstrap-servers", () -> proxy.getContainerIpAddress() + ":" + proxy.getProxyPort());
    }

    @Autowired
    private OrderProducer producer;

    @Autowired
    private OrderConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer.resetLatch(1);
    }

    @Test
    void testResilienceDuringNetworkLatency() throws Exception {
        // Add 2 seconds latency downstream
        proxy.toxics().latency("latency-toxic", ToxicDirection.DOWNSTREAM, 2000);

        producer.sendOrder("chaos-order-1");

        // Should eventually be consumed despite latency
        boolean consumed = consumer.getLatch().await(10, TimeUnit.SECONDS);
        assertThat(consumed).isTrue();

        proxy.toxics().get("latency-toxic").remove();
    }
}
