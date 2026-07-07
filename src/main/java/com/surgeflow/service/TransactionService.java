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
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {

    private final RedisTemplate<String, String> redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${surgeflow.kafka.topic}")
    private String transactionTopic;

    private static final String BALANCE_KEY_PREFIX = "surgeflow:balance:";

    /**
     * Core transaction processing:
     * 1. Reject unknown accounts — no silent auto-seeding
     * 2. Atomic Redis INCRBY for lockless balance mutation
     * 3. Kafka publish with explicit failure logging
     */
    public TransactionResponse processTransaction(TransactionRequest request) {
        long startTime = System.currentTimeMillis();
        String transactionId = UUID.randomUUID().toString();
        String balanceKey = BALANCE_KEY_PREFIX + request.getAccountId();

        try {
            // Reject unknown accounts — do not silently create them
            String existing = redisTemplate.opsForValue().get(balanceKey);
            if (existing == null) {
                return buildResponse(transactionId, request, "REJECTED",
                        "Account not found. Use /seed to initialise a test account.", startTime);
            }

            // Fix: HALF_UP rounding instead of truncation
            long amountInPaise = request.getAmount()
                    .multiply(BigDecimal.valueOf(100))
                    .setScale(0, RoundingMode.HALF_UP)
                    .longValue();

            long newBalance;
            if (request.getType() == Transaction.TransactionType.DEBIT) {
                newBalance = redisTemplate.opsForValue().increment(balanceKey, -amountInPaise);
                if (newBalance < 0) {
                    redisTemplate.opsForValue().increment(balanceKey, amountInPaise);
                    return buildResponse(transactionId, request, "REJECTED",
                            "Insufficient balance", startTime);
                }
            } else {
                newBalance = redisTemplate.opsForValue().increment(balanceKey, amountInPaise);
            }

            // Publish to Kafka with explicit failure handling
            Transaction transaction = Transaction.builder()
                    .transactionId(transactionId)
                    .accountId(request.getAccountId())
                    .amount(request.getAmount())
                    .type(request.getType())
                    .status(Transaction.TransactionStatus.APPROVED)
                    .createdAt(LocalDateTime.now())
                    .build();

            String payload = objectMapper.writeValueAsString(transaction);

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(transactionTopic, request.getAccountId(), payload);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    // Redis balance moved but Kafka failed — log for reconciliation
                    log.error("KAFKA_SEND_FAILURE txn={} account={} amount={} error={} " +
                                    "ACTION_REQUIRED: balance updated in Redis but not persisted to Postgres",
                            transactionId, request.getAccountId(), request.getAmount(), ex.getMessage());
                } else {
                    log.info("Transaction {} approved | Account: {} | Balance: ₹{}",
                            transactionId, request.getAccountId(),
                            BigDecimal.valueOf(newBalance).divide(BigDecimal.valueOf(100)));
                }
            });

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
     * Returns balance for known accounts only.
     * Returns null for unknown accounts — callers handle the 404.
     */
    public BigDecimal getBalance(String accountId) {
        String value = redisTemplate.opsForValue().get(BALANCE_KEY_PREFIX + accountId);
        if (value == null) {
            return null;
        }
        return new BigDecimal(value).divide(BigDecimal.valueOf(100));
    }

    /**
     * Seed an account with a specific balance.
     * Only entry point for account creation — not triggered by balance reads or transactions.
     */
    public void seedAccount(String accountId, BigDecimal balance) {
        String balanceKey = BALANCE_KEY_PREFIX + accountId;
        long balanceInPaise = balance
                .multiply(BigDecimal.valueOf(100))
                .setScale(0, RoundingMode.HALF_UP)
                .longValue();
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
