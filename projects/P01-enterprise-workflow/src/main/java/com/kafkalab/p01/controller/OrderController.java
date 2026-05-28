package com.kafkalab.p01.controller;

import com.kafkalab.p01.model.OrderEvent;
import com.kafkalab.p01.model.OrderRequest;
import com.kafkalab.p01.model.OrderResponse;
import com.kafkalab.p01.producer.OrderProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping
    public Mono<ResponseEntity<OrderResponse>> createOrder(@RequestBody Mono<OrderRequest> requestMono) {
        return requestMono.flatMap(request -> {
            String eventId = UUID.randomUUID().toString();
            OrderEvent event = new OrderEvent(eventId, request, "CREATED");
            
            // Publish to Kafka non-blockingly, then return 202 Accepted
            return orderProducer.sendOrderEvent(event)
                    .then(Mono.just(ResponseEntity
                            .status(HttpStatus.ACCEPTED)
                            .body(new OrderResponse(request.getOrderId(), "ACCEPTED", "Order is being processed"))));
        });
    }
}
