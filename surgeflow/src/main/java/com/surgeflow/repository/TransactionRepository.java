package com.surgeflow.repository;

import com.surgeflow.model.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, Long> {

    Optional<Transaction> findByTransactionId(String transactionId);

    List<Transaction> findByAccountIdOrderByCreatedAtDesc(String accountId);

    @Query("SELECT t FROM Transaction t WHERE t.status = 'PENDING' ORDER BY t.createdAt ASC")
    List<Transaction> findPendingTransactions();

    @Modifying
    @Query("UPDATE Transaction t SET t.status = :status, t.processedAt = CURRENT_TIMESTAMP WHERE t.transactionId = :transactionId")
    int updateTransactionStatus(String transactionId, Transaction.TransactionStatus status);
}
