package com.kafkalab.reactive.producer;

import com.kafkalab.reactive.model.Transaction;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
public class ReactiveProducer {
    
    private static final Logger log = LoggerFactory.getLogger(ReactiveProducer.class);
    private final KafkaSender<String, Transaction> kafkaSender;

    public ReactiveProducer(KafkaSender<String, Transaction> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    public Mono<Void> sendTransaction(Transaction transaction) {
        ProducerRecord<String, Transaction> producerRecord = new ProducerRecord<>(
                "transactions", transaction.getTransactionId(), transaction);
                
        SenderRecord<String, Transaction, String> senderRecord = SenderRecord.create(
                producerRecord, transaction.getTransactionId());

        return kafkaSender.send(Mono.just(senderRecord))
                .doOnNext(result -> log.info("Successfully sent transaction {} to partition {} offset {}", 
                        result.correlationMetadata(), result.recordMetadata().partition(), result.recordMetadata().offset()))
                .doOnError(e -> log.error("Failed to send transaction {}", transaction.getTransactionId(), e))
                .then();
    }
}
