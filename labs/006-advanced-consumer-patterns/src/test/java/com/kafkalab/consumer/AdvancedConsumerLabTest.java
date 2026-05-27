package com.kafkalab.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AdvancedConsumerLabTest {

    private static KafkaContainer kafka;
    private static AdminClient adminClient;

    @BeforeAll
    public static void setUpClass() {
        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));
        kafka.start();

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        adminClient = AdminClient.create(adminProps);
    }

    @AfterAll
    public static void tearDownClass() {
        if (adminClient != null) adminClient.close();
        if (kafka != null) kafka.stop();
    }

    @Test
    public void testManualCommitSuccess() throws InterruptedException, ExecutionException {
        String topic = "manual-commit-topic";
        adminClient.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1))).all().get();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, "key1", "val1")).get();
            producer.send(new ProducerRecord<>(topic, "key2", "val2")).get();
        }

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-manual-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ManualCommitConsumer manualConsumer = new ManualCommitConsumer(consumerProps, topic);
        Thread t = new Thread(manualConsumer);
        t.start();

        // Let it process
        Thread.sleep(3000);
        manualConsumer.stop();
        t.join();

        // The consumer should shut down gracefully without errors.
    }

    @Test
    public void testManualCommitWithPoisonPill() throws InterruptedException, ExecutionException {
        String topic = "poison-topic";
        adminClient.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1))).all().get();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, "key", "poison_pill")).get();
        }

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-poison-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ManualCommitConsumer manualConsumer = new ManualCommitConsumer(consumerProps, topic);
        
        assertThatThrownBy(manualConsumer::run)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Simulated processing failure!");
                
        // Because it crashed before commitSync/commitAsync, offsets aren't committed.
        // A new consumer in the same group would re-read "poison_pill".
    }

    @Test
    public void testRebalanceListenerInvocation() throws InterruptedException, ExecutionException {
        String topic = "rebalance-topic";
        // Topic with 2 partitions to allow 2 consumers to work
        adminClient.createTopics(Collections.singleton(new NewTopic(topic, 2, (short) 1))).all().get();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        String groupId = "group-rebalance-" + UUID.randomUUID();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Force quick rebalances for tests
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");

        RebalanceListenerConsumer consumer1 = new RebalanceListenerConsumer((Properties) consumerProps.clone(), topic);
        Thread t1 = new Thread(consumer1);
        t1.start();

        // Wait for first assignment
        Thread.sleep(3000);
        assertThat(consumer1.getAssignedCount()).isGreaterThan(0);

        // Start second consumer to force a rebalance
        RebalanceListenerConsumer consumer2 = new RebalanceListenerConsumer((Properties) consumerProps.clone(), topic);
        Thread t2 = new Thread(consumer2);
        t2.start();

        // Wait for rebalance to finish
        Thread.sleep(8000);

        // consumer1 should have had its partitions revoked
        assertThat(consumer1.getRevokedCount()).isGreaterThan(0);

        consumer1.stop();
        consumer2.stop();
        t1.join();
        t2.join();
    }
}
