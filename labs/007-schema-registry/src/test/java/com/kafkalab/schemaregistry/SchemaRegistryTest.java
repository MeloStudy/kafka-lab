package com.kafkalab.schemaregistry;

import com.kafkalab.schemaregistry.avro.OrderV1;
import com.kafkalab.schemaregistry.avro.OrderV3;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SchemaRegistryTest {

    private static Network network;
    private static KafkaContainer kafka;
    private static GenericContainer<?> schemaRegistry;
    private static String schemaRegistryUrl;
    private static final String TOPIC = "orders-topic";

    @BeforeAll
    public static void setUp() {
        network = Network.newNetwork();

        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka");
        kafka.start();

        schemaRegistry = new GenericContainer<>(DockerImageName.parse("bitnami/schema-registry:7.6.0"))
                .withNetwork(network)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_KAFKA_BROKERS", "PLAINTEXT://kafka:9092")
                .dependsOn(kafka);
        schemaRegistry.start();

        schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    }

    @AfterAll
    public static void tearDown() {
        if (schemaRegistry != null) schemaRegistry.stop();
        if (kafka != null) kafka.stop();
        if (network != null) network.close();
    }

    @Test
    public void testProduceAndConsumeOrderV1() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        KafkaProducer<String, OrderV1> producer = new KafkaProducer<>(producerProps);
        
        OrderV1 order = OrderV1.newBuilder()
                .setOrderId("ord-123")
                .setProductId("prod-abc")
                .setAmount(99.99)
                .build();
                
        producer.send(new ProducerRecord<>(TOPIC, order.getOrderId(), order));
        producer.flush();
        producer.close();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        KafkaConsumer<String, OrderV1> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        ConsumerRecords<String, OrderV1> records = consumer.poll(Duration.ofSeconds(10));
        assertThat(records.count()).isGreaterThan(0);
        
        ConsumerRecord<String, OrderV1> record = records.iterator().next();
        assertThat(record.value().getOrderId()).isEqualTo("ord-123");
        
        consumer.close();
    }

    @Test
    public void testSchemaEvolutionFailureWithOrderV3() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(producerProps)) {
            OrderV1 order1 = OrderV1.newBuilder()
                    .setOrderId("ord-1")
                    .setProductId("p-1")
                    .setAmount(10.0)
                    .build();
            producer.send(new ProducerRecord<>(TOPIC + "-evolve", order1.getOrderId(), order1));
            producer.flush();

            OrderV3 order3 = OrderV3.newBuilder()
                    .setOrderId("ord-3")
                    .setProductId("p-3")
                    .setAmount(30.0)
                    .setLoyaltyPoints(5)
                    .setDiscount(5.0)
                    .build();

            assertThatThrownBy(() -> {
                producer.send(new ProducerRecord<>(TOPIC + "-evolve", order3.getOrderId(), order3));
            }).isInstanceOf(SerializationException.class);
        }
    }
}
