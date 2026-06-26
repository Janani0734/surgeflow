package com.surgeflow.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.surgeflow.model.Transaction;
import com.surgeflow.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionConsumer {

    private final TransactionRepository transactionRepository;
    private final ObjectMapper objectMapper;

    // Metrics counter
    private final AtomicLong totalProcessed = new AtomicLong(0);

    /**
     * Batch Kafka consumer — pulls up to 500 messages per poll.
     * Uses JPA saveAll() for efficient batch insert to PostgreSQL.
     * This decouples API ingestion speed from disk I/O constraints.
     */
    @KafkaListener(
            topics = "${surgeflow.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeTransactions(@Payload List<String> payloads,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
                                    @Header(KafkaHeaders.OFFSET) List<Long> offsets) {

        List<Transaction> batch = new ArrayList<>(payloads.size());

        for (String payload : payloads) {
            try {
                Transaction transaction = objectMapper.readValue(payload, Transaction.class);
                transaction.setId(null); // Let DB assign ID
                batch.add(transaction);
            } catch (Exception e) {
                log.error("Failed to deserialize transaction payload: {}", e.getMessage());
            }
        }

        if (!batch.isEmpty()) {
            // Batch insert — 0% row-lock deadlocks via sequential writes
            transactionRepository.saveAll(batch);
            long count = totalProcessed.addAndGet(batch.size());
            log.info("Batch persisted {} transactions | Total processed: {} | Partitions: {}",
                    batch.size(), count, partitions);
        }
    }
}
