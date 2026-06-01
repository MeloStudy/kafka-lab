package com.kafkalab.springboot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProducer {

    private final KafkaTemplate<String, Payment> kafkaTemplate;
    private static final String TOPIC = "payments";

    public void sendPayment(Payment payment) {
        log.info("Sending payment: {}", payment);
        kafkaTemplate.send(TOPIC, payment.getId(), payment);
    }
}
