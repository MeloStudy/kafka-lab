package com.kafkalab.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class HighThroughputProducer {

    public static KafkaProducer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // High Throughput configurations
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32KB batch size
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20); // Wait up to 20ms to batch
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Compress batch
        
        return new KafkaProducer<>(props);
    }
}
