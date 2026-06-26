package com.surgeflow.controller;

import com.surgeflow.model.TransactionRequest;
import com.surgeflow.model.TransactionResponse;
import com.surgeflow.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Slf4j
public class TransactionController {

    private final TransactionService transactionService;

    /**
     * POST /api/v1/transactions
     * Main ingestion endpoint — handles 50K+ RPS via Java 21 Virtual Threads
     */
    @PostMapping("/transactions")
    public ResponseEntity<TransactionResponse> processTransaction(
            @RequestBody TransactionRequest request) {

        TransactionResponse response = transactionService.processTransaction(request);
        return ResponseEntity.ok(response);
    }

    /**
     * GET /api/v1/accounts/{accountId}/balance
     * Sub-2ms balance check directly from Redis
     */
    @GetMapping("/accounts/{accountId}/balance")
    public ResponseEntity<Map<String, Object>> getBalance(@PathVariable String accountId) {
        BigDecimal balance = transactionService.getBalance(accountId);
        return ResponseEntity.ok(Map.of(
                "accountId", accountId,
                "balance", balance,
                "currency", "INR",
                "source", "Redis-Cache"
        ));
    }

    /**
     * POST /api/v1/accounts/{accountId}/seed
     * Seed account balance for testing surge scenarios
     */
    @PostMapping("/accounts/{accountId}/seed")
    public ResponseEntity<Map<String, Object>> seedAccount(
            @PathVariable String accountId,
            @RequestParam(defaultValue = "100000") BigDecimal balance) {

        transactionService.seedAccount(accountId, balance);
        return ResponseEntity.ok(Map.of(
                "accountId", accountId,
                "seededBalance", balance,
                "message", "Account seeded successfully"
        ));
    }

    /**
     * GET /api/v1/health
     * Quick health check
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "engine", "SurgeFlow v1.0",
                "threads", "Java 21 Virtual Threads",
                "cache", "Redis",
                "queue", "Apache Kafka"
        ));
    }
}
