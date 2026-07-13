package com.surgeflow.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.surgeflow.model.Transaction;
import com.surgeflow.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionConsumer {

    private final TransactionRepository transactionRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${surgeflow.kafka.topic}",
                   groupId = "${spring.kafka.consumer.group-id}",
                   concurrency = "3")
    public void consume(String payload) {
        try {
            Transaction transaction = objectMapper.readValue(payload, Transaction.class);
            transactionRepository.save(transaction);
            log.debug("Persisted transaction {}", transaction.getTransactionId());
        } catch (Exception e) {
            log.error("Failed to persist transaction: {}", e.getMessage());
        }
    }
}
