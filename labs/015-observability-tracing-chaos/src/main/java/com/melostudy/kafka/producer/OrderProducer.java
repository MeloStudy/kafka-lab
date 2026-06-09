package com.melostudy.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendOrder(String orderId) {
        log.info("Producing order: {}", orderId);
        kafkaTemplate.send("orders", orderId, "Order details for " + orderId)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Successfully produced order: {} to partition: {} with offset: {}", 
                        orderId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                } else {
                    log.error("Failed to produce order: {}", orderId, ex);
                }
            });
    }
}
