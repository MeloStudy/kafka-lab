package com.kafkalab.reactive.consumer;

import com.kafkalab.reactive.model.Transaction;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ReactiveConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReactiveConsumer.class);
    private final KafkaReceiver<String, Transaction> kafkaReceiver;
    
    // Hook for integration testing
    public static final AtomicInteger MESSAGE_COUNT = new AtomicInteger(0);

    public ReactiveConsumer(KafkaReceiver<String, Transaction> kafkaReceiver) {
        this.kafkaReceiver = kafkaReceiver;
    }

    @PostConstruct
    public void consume() {
        kafkaReceiver.receive()
                // Process sequentially and guarantee offset ordering
                .concatMap(record -> processRecord(record)
                        // Acknowledge the offset only AFTER successful processing
                        .doOnSuccess(v -> record.receiverOffset().acknowledge())
                        .thenReturn(record)
                )
                .subscribe();
    }

    private reactor.core.publisher.Mono<Void> processRecord(ReceiverRecord<String, Transaction> record) {
        return reactor.core.publisher.Mono.just(record)
                // Simulate a slow downstream to demonstrate backpressure pausing poll()
                .delayElement(Duration.ofMillis(500))
                .doOnNext(r -> {
                    log.info("Processed transaction: {} from partition {} offset {}", 
                            r.value().getTransactionId(), r.partition(), r.offset());
                    
                    MESSAGE_COUNT.incrementAndGet();
                })
                .then();
    }
}
