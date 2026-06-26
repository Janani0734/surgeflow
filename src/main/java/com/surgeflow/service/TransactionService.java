package com.surgeflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.surgeflow.model.Transaction;
import com.surgeflow.model.TransactionRequest;
import com.surgeflow.model.TransactionResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {

    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${surgeflow.kafka.topic}")
    private String transactionTopic;

    // Redis key prefix for account balances
    private static final String BALANCE_KEY_PREFIX = "surgeflow:balance:";
    // Default starting balance for new accounts (in paise/cents)
    private static final long DEFAULT_BALANCE = 100_000_00L; // ₹1,00,000

    /**
     * Core transaction processing:
     * 1. Atomic Redis HINCRBY for lockless inventory check
     * 2. Publish to Kafka for async PostgreSQL persistence
     */
    public TransactionResponse processTransaction(TransactionRequest request) {
        long startTime = System.currentTimeMillis();
        String transactionId = UUID.randomUUID().toString();
        String balanceKey = BALANCE_KEY_PREFIX + request.getAccountId();

        try {
            // Step 1: Initialize account balance in Redis if not exists
            redisTemplate.opsForValue().setIfAbsent(balanceKey,
                    String.valueOf(DEFAULT_BALANCE));

            // Step 2: Atomic balance mutation via Redis INCRBY
            // This is lockless — Redis single-threaded event loop serializes ops
            long amountInPaise = request.getAmount()
                    .multiply(BigDecimal.valueOf(100))
                    .longValue();

            long newBalance;
            if (request.getType() == Transaction.TransactionType.DEBIT) {
                // Atomically decrement — if negative, rollback
                newBalance = redisTemplate.opsForValue()
                        .increment(balanceKey, -amountInPaise);

                if (newBalance < 0) {
                    // Rollback: restore the decremented amount
                    redisTemplate.opsForValue().increment(balanceKey, amountInPaise);
                    return buildResponse(transactionId, request, "REJECTED",
                            "Insufficient balance", startTime);
                }
            } else {
                // Credit: atomically increment
                newBalance = redisTemplate.opsForValue()
                        .increment(balanceKey, amountInPaise);
            }

            // Step 3: Publish approved transaction to Kafka (non-blocking)
            Transaction transaction = Transaction.builder()
                    .transactionId(transactionId)
                    .accountId(request.getAccountId())
                    .amount(request.getAmount())
                    .type(request.getType())
                    .status(Transaction.TransactionStatus.APPROVED)
                    .createdAt(LocalDateTime.now())
                    .build();

            String payload = objectMapper.writeValueAsString(transaction);
            kafkaTemplate.send(transactionTopic, request.getAccountId(), payload);

            log.info("Transaction {} approved | Account: {} | Balance: {}",
                    transactionId, request.getAccountId(),
                    BigDecimal.valueOf(newBalance).divide(BigDecimal.valueOf(100)));

            return buildResponse(transactionId, request, "APPROVED",
                    "Transaction approved. New balance: ₹" +
                            BigDecimal.valueOf(newBalance).divide(BigDecimal.valueOf(100)),
                    startTime);

        } catch (Exception e) {
            log.error("Transaction {} failed: {}", transactionId, e.getMessage());
            return buildResponse(transactionId, request, "FAILED",
                    "System error: " + e.getMessage(), startTime);
        }
    }

    /**
     * Get current account balance from Redis (sub-2ms)
     */
    public BigDecimal getBalance(String accountId) {
        String balanceKey = BALANCE_KEY_PREFIX + accountId;
        String value = redisTemplate.opsForValue().get(balanceKey);

        if (value == null) {
            // Initialize with default balance
            redisTemplate.opsForValue().set(balanceKey, String.valueOf(DEFAULT_BALANCE));
            return BigDecimal.valueOf(DEFAULT_BALANCE).divide(BigDecimal.valueOf(100));
        }

        return new BigDecimal(value).divide(BigDecimal.valueOf(100));
    }

    /**
     * Seed an account with a specific balance (for testing)
     */
    public void seedAccount(String accountId, BigDecimal balance) {
        String balanceKey = BALANCE_KEY_PREFIX + accountId;
        long balanceInPaise = balance.multiply(BigDecimal.valueOf(100)).longValue();
        redisTemplate.opsForValue().set(balanceKey, String.valueOf(balanceInPaise));
        log.info("Seeded account {} with ₹{}", accountId, balance);
    }

    private TransactionResponse buildResponse(String transactionId,
                                               TransactionRequest request,
                                               String status, String message,
                                               long startTime) {
        return TransactionResponse.builder()
                .transactionId(transactionId)
                .accountId(request.getAccountId())
                .amount(request.getAmount())
                .status(status)
                .message(message)
                .timestamp(LocalDateTime.now())
                .processingTimeMs(System.currentTimeMillis() - startTime)
                .build();
    }
}
