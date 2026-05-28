package com.kafkalab.p01.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.p01.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class OrderConsumer {

    @Value("${kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.consumer.topic.inbound}")
    private String inboundTopic;

    @Value("${kafka.consumer.topic.confirmed}")
    private String confirmedTopic;

    @Value("${kafka.consumer.topic.failed}")
    private String failedTopic;

    private KafkaReceiver<String, String> receiver;
    private KafkaSender<String, String> sender;
    private final ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        // Configure Consumer
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProps)
                .subscription(Collections.singleton(inboundTopic));
        
        receiver = KafkaReceiver.create(receiverOptions);

        // Configure Producer (for confirmed/failed events)
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        SenderOptions<String, String> senderOptions = SenderOptions.create(producerProps);
        sender = KafkaSender.create(senderOptions);

        // Start consuming
        consumeMessages();
    }

    private void consumeMessages() {
        receiver.receive()
                .flatMap(record -> {
                    try {
                        OrderEvent event = mapper.readValue(record.value(), OrderEvent.class);
                        // Simulate processing: if amount > 1000, reject it.
                        if (event.getPayload().getAmount() > 1000) {
                            event.setStatus("FAILED");
                            return processAndSend(event, failedTopic, record);
                        } else {
                            event.setStatus("CONFIRMED");
                            return processAndSend(event, confirmedTopic, record);
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to parse event: " + e.getMessage());
                        record.receiverOffset().acknowledge(); // Acknowledge bad message to not get stuck
                        return Flux.empty();
                    }
                })
                .subscribe();
    }

    private Flux<?> processAndSend(OrderEvent event, String targetTopic, reactor.kafka.receiver.ReceiverRecord<String, String> originalRecord) {
        try {
            String json = mapper.writeValueAsString(event);
            ProducerRecord<String, String> prodRecord = new ProducerRecord<>(targetTopic, event.getPayload().getOrderId(), json);
            SenderRecord<String, String, Void> senderRecord = SenderRecord.create(prodRecord, null);
            
            return sender.send(Flux.just(senderRecord))
                    .doOnNext(result -> originalRecord.receiverOffset().acknowledge());
        } catch (Exception e) {
            return Flux.error(e);
        }
    }

    @PreDestroy
    public void close() {
        if (sender != null) sender.close();
    }
}
