package com.surgeflow.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionResponse {
    private String transactionId;
    private String accountId;
    private BigDecimal amount;
    private String status;
    private String message;
    private LocalDateTime timestamp;
    private long processingTimeMs;
}
