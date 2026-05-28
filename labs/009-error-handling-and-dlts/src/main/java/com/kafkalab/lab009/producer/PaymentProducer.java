package com.kafkalab.lab009.producer;

import com.kafkalab.lab009.model.PaymentEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentProducer {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    public static final String TOPIC = "payments";

    public PaymentProducer(KafkaTemplate<String, PaymentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPayment(PaymentEvent event) {
        kafkaTemplate.send(TOPIC, event.getId(), event);
    }
}
