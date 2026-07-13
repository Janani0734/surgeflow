package com.surgeflow.model;

import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "transactions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    @Id
    private String transactionId;

    private String accountId;
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private TransactionType type;

    @Enumerated(EnumType.STRING)
    private TransactionStatus status;

    private LocalDateTime createdAt;

    public enum TransactionType { DEBIT, CREDIT }
    public enum TransactionStatus { APPROVED, REJECTED, FAILED }
}
