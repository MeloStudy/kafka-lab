package com.kafkalab.p01.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic inboundTopic() {
        return TopicBuilder.name("orders.inbound")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic confirmedTopic() {
        return TopicBuilder.name("orders.confirmed")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic failedTopic() {
        return TopicBuilder.name("orders.failed")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
