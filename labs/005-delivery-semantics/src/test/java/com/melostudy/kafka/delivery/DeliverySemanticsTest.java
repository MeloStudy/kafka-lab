package com.melostudy.kafka.delivery;

import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DeliverySemanticsTest {

    private static KafkaContainer kafka;
    private static String bootstrapServers;

    @BeforeAll
    static void setUp() {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
        kafka.start();
        bootstrapServers = kafka.getBootstrapServers();
    }

    @AfterAll
    static void tearDown() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    @Test
    void testReadCommittedIsolation() {
        String topic = "transactions-topic";
        TransactionalProducer producer = new TransactionalProducer(bootstrapServers, "test-tx-1");
        
        // 1. Send 3 messages successfully
        producer.processAndSend(topic, new String[]{"Msg1", "Msg2", "Msg3"}, false);
        
        // 2. Try to send 3 more but abort due to error
        producer.processAndSend(topic, new String[]{"Abort1", "Abort2", "Abort3"}, true);
        
        // 3. Read with read_committed
        ReadCommittedConsumer committedConsumer = new ReadCommittedConsumer(bootstrapServers, "group-1", "read_committed");
        List<String> committedMessages = committedConsumer.readMessages(topic, 3);
        assertThat(committedMessages).containsExactly("Msg1", "Msg2", "Msg3");
        committedConsumer.close();
        
        // 4. Read with read_uncommitted
        ReadCommittedConsumer uncommittedConsumer = new ReadCommittedConsumer(bootstrapServers, "group-2", "read_uncommitted");
        List<String> uncommittedMessages = uncommittedConsumer.readMessages(topic, 6); // Try to read all 6
        assertThat(uncommittedMessages).contains("Abort1", "Abort2", "Abort3");
        uncommittedConsumer.close();
        
        producer.close();
    }

    @Test
    void testZombieFencing() {
        String topic = "zombie-topic";
        String txId = "zombie-tx";
        
        TransactionalProducer producerA = new TransactionalProducer(bootstrapServers, txId);
        // Producer A initializes transactions, gets PID + Epoch 0
        
        TransactionalProducer producerB = new TransactionalProducer(bootstrapServers, txId);
        // Producer B initializes transactions with same ID, gets PID + Epoch 1
        // Coordinator expects Epoch 1 now.
        
        // Producer A tries to begin and commit a transaction using Epoch 0
        assertThatThrownBy(() -> producerA.processAndSend(topic, new String[]{"Ghost Msg"}, false))
                .isInstanceOf(ProducerFencedException.class);
        
        // Producer B can successfully send
        producerB.processAndSend(topic, new String[]{"Legit Msg"}, false);
        
        ReadCommittedConsumer consumer = new ReadCommittedConsumer(bootstrapServers, "group-zombie", "read_committed");
        List<String> msgs = consumer.readMessages(topic, 1);
        assertThat(msgs).containsExactly("Legit Msg");
        
        producerB.close();
        consumer.close();
    }
}
