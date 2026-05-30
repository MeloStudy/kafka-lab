package com.kafkalab.reactive.controller;

import com.kafkalab.reactive.model.Transaction;
import com.kafkalab.reactive.producer.ReactiveProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final ReactiveProducer reactiveProducer;

    public TransactionController(ReactiveProducer reactiveProducer) {
        this.reactiveProducer = reactiveProducer;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> processTransaction(@RequestBody Transaction transaction) {
        return reactiveProducer.sendTransaction(transaction)
                .thenReturn(ResponseEntity.accepted().body("Accepted transaction " + transaction.getTransactionId()))
                .onErrorResume(e -> Mono.just(ResponseEntity.internalServerError().body("Error: " + e.getMessage())));
    }
}
