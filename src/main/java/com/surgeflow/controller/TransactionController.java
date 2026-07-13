package com.surgeflow.controller;

import com.surgeflow.model.TransactionRequest;
import com.surgeflow.model.TransactionResponse;
import com.surgeflow.service.TransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionService transactionService;

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
            "engine", "SurgeFlow v1.0",
            "threads", "Java 21 Virtual Threads",
            "queue", "Apache Kafka",
            "status", "UP",
            "cache", "Redis"
        ));
    }

    @PostMapping("/transactions")
    public ResponseEntity<TransactionResponse> processTransaction(
            @Valid @RequestBody TransactionRequest request) {
        TransactionResponse response = transactionService.processTransaction(request);
        int httpStatus = "APPROVED".equals(response.getStatus()) ? 200 : 400;
        return ResponseEntity.status(httpStatus).body(response);
    }

    @GetMapping("/accounts/{accountId}/balance")
    public ResponseEntity<?> getBalance(@PathVariable String accountId) {
        BigDecimal balance = transactionService.getBalance(accountId);
        if (balance == null) {
            return ResponseEntity.status(404).body(Map.of("error", "Account not found"));
        }
        return ResponseEntity.ok(Map.of(
            "accountId", accountId,
            "balance", balance,
            "currency", "INR",
            "source", "Redis-Cache"
        ));
    }

    @PostMapping("/accounts/{accountId}/seed")
    public ResponseEntity<Map<String, Object>> seedAccount(
            @PathVariable String accountId,
            @RequestParam(defaultValue = "10000") BigDecimal balance) {
        transactionService.seedAccount(accountId, balance);
        return ResponseEntity.ok(Map.of(
            "accountId", accountId,
            "seededBalance", balance,
            "message", "Account seeded successfully"
        ));
    }
}
