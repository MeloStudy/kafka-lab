package com.kafkalab.springboot;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Slf4j
@Service
public class PaymentConsumer {

    // Latch used purely for integration testing
    @Getter
    private CountDownLatch latch = new CountDownLatch(1);

    @Getter
    private Payment lastReceivedPayment;

    @KafkaListener(topics = "payments", groupId = "payment-consumers")
    public void consume(Payment payment) {
        log.info("Received payment payload: {}", payment);
        this.lastReceivedPayment = payment;
        latch.countDown();
    }

    public void resetLatch() {
        latch = new CountDownLatch(1);
    }
}
