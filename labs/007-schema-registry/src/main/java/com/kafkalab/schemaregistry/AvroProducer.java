package com.kafkalab.schemaregistry;

import com.kafkalab.schemaregistry.avro.OrderV1;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        try (KafkaProducer<String, OrderV1> producer = new KafkaProducer<>(props)) {
            OrderV1 order = OrderV1.newBuilder()
                    .setOrderId("ord-100")
                    .setProductId("prod-abc")
                    .setAmount(150.50)
                    .build();

            ProducerRecord<String, OrderV1> record = new ProducerRecord<>("orders-topic", order.getOrderId(), order);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Produced order: " + order.getOrderId() + " at offset: " + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }
    }
}
