package com.kafkalab.p01.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.p01.model.OrderEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

@Service
public class OrderProducer {

    @Value("${kafka.producer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.topic.inbound}")
    private String inboundTopic;

    private KafkaSender<String, String> sender;
    private final ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Important: Idempotent producer is enabled by default in Kafka 3.x, ensuring exactly-once semantics per partition
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        SenderOptions<String, String> senderOptions = SenderOptions.create(props);
        sender = KafkaSender.create(senderOptions);
    }

    public Mono<Void> sendOrderEvent(OrderEvent event) {
        return Mono.fromCallable(() -> mapper.writeValueAsString(event))
                .map(json -> new ProducerRecord<>(inboundTopic, event.getPayload().getOrderId(), json))
                .map(record -> SenderRecord.create(record, event.getEventId()))
                .as(sender::send)
                .doOnError(e -> System.err.println("Error sending order event: " + e.getMessage()))
                .then(); // Return empty Mono when done
    }

    @PreDestroy
    public void close() {
        if (sender != null) {
            sender.close();
        }
    }
}
