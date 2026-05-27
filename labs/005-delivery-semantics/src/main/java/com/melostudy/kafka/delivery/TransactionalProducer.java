package com.melostudy.kafka.delivery;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionalProducer {

    private final KafkaProducer<String, String> producer;

    public TransactionalProducer(String bootstrapServers, String transactionalId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Exactly Once Semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        
        this.producer = new KafkaProducer<>(props);
        this.producer.initTransactions();
    }

    public void processAndSend(String topic, String[] messages, boolean simulateError) {
        producer.beginTransaction();
        try {
            for (String msg : messages) {
                producer.send(new ProducerRecord<>(topic, msg));
            }
            if (simulateError) {
                producer.flush(); // Ensure the broker receives the uncommitted messages before the crash
                throw new RuntimeException("Simulated error in processing loop");
            }
            producer.commitTransaction();
        } catch (ProducerFencedException e) {
            // Fenced off by another producer instance with the same transactional.id
            producer.close();
            throw e;
        } catch (Exception e) {
            producer.abortTransaction();
        }
    }

    public void close() {
        producer.close();
    }
}
