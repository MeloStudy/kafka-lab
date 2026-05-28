package com.kafkalab.lab009.consumer;

import com.kafkalab.lab009.model.PaymentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    // For testing verification purposes
    public final List<PaymentEvent> processedEvents = new ArrayList<>();
    public final List<PaymentEvent> dltEvents = new ArrayList<>();

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            autoCreateTopics = "true",
            exclude = {IllegalArgumentException.class} // Fatal exception, goes straight to DLT
    )
    @KafkaListener(topics = "payments", groupId = "payment-group")
    public void consumePayment(PaymentEvent event) {
        log.info("Received Payment: {}", event);

        // Simulate a transient error
        if ("TRANSIENT_ERROR".equals(event.getStatus())) {
            log.warn("Simulating transient failure for event: {}", event.getId());
            throw new RuntimeException("Temporary network issue");
        }

        // Simulate a fatal error
        if ("FATAL_ERROR".equals(event.getStatus())) {
            log.error("Simulating fatal failure for event: {}", event.getId());
            throw new IllegalArgumentException("Invalid payload structure");
        }

        // Success
        log.info("Successfully processed payment: {}", event.getId());
        processedEvents.add(event);
    }

    @DltHandler
    public void processDltMessage(PaymentEvent event,
                                  @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                  @Header(KafkaHeaders.OFFSET) long offset,
                                  @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String errorMessage) {
        log.error("DLT Received Event: {} from topic: {} at offset: {}. Error: {}", event, topic, offset, errorMessage);
        dltEvents.add(event);
    }
}
