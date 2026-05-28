package com.kafkalab.lab009;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class Lab009Application {
    public static void main(String[] args) {
        SpringApplication.run(Lab009Application.class, args);
    }
}
