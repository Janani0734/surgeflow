package com.surgeflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${surgeflow.kafka.topic}")
    private String transactionTopic;

    @Bean
    public NewTopic transactionTopic() {
        return TopicBuilder.name(transactionTopic)
                .partitions(6)       // 6 partitions for parallelism
                .replicas(1)         // 1 replica for local dev
                .build();
    }
}
