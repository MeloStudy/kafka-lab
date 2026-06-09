package com.melostudy.kafka.controller;

import com.melostudy.kafka.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestParam(required = false) String id) {
        String orderId = (id != null) ? id : UUID.randomUUID().toString();
        orderProducer.sendOrder(orderId);
        return ResponseEntity.ok("Order sent: " + orderId);
    }
}
