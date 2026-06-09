package com.melostudy.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Slf4j
@Component
public class OrderConsumer {

    private CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(topics = "orders", groupId = "order-group")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("Consumed order: {} from partition: {} with offset: {}", 
            record.key(), record.partition(), record.offset());
        latch.countDown();
    }

    public void resetLatch(int count) {
        latch = new CountDownLatch(count);
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
