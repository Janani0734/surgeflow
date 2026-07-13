package com.surgeflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.surgeflow.model.Transaction;
import com.surgeflow.model.TransactionRequest;
import com.surgeflow.model.TransactionResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("TransactionService Unit Tests")
class TransactionServiceTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private TransactionService transactionService;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    @DisplayName("Should APPROVE debit when account exists and balance is sufficient")
    void shouldApproveDebitWhenSufficientBalance() throws Exception {
        when(valueOperations.get(anyString())).thenReturn("10000000"); // account exists
        when(valueOperations.increment(anyString(), anyLong())).thenReturn(9950000L);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doReturn(CompletableFuture.completedFuture(null))
                .when(kafkaTemplate).send(any(), any(), any());

        TransactionRequest request = TransactionRequest.builder()
                .accountId("ACC001")
                .amount(new BigDecimal("500.00"))
                .type(Transaction.TransactionType.DEBIT)
                .build();

        TransactionResponse response = transactionService.processTransaction(request);

        assertThat(response.getStatus()).isEqualTo("APPROVED");
        assertThat(response.getAccountId()).isEqualTo("ACC001");
    }

    @Test
    @DisplayName("Should REJECT debit when balance is insufficient")
    void shouldRejectDebitWhenInsufficientBalance() {
        when(valueOperations.get(anyString())).thenReturn("1000"); // account exists, low balance
        when(valueOperations.increment(anyString(), anyLong()))
                .thenReturn(-99999900L)
                .thenReturn(1L); // rollback

        TransactionRequest request = TransactionRequest.builder()
                .accountId("ACC002")
                .amount(new BigDecimal("999999.00"))
                .type(Transaction.TransactionType.DEBIT)
                .build();

        TransactionResponse response = transactionService.processTransaction(request);

        assertThat(response.getStatus()).isEqualTo("REJECTED");
    }

    @Test
    @DisplayName("Should REJECT when account does not exist")
    void shouldRejectWhenAccountNotFound() {
        when(valueOperations.get(anyString())).thenReturn(null); // account does not exist

        TransactionRequest request = TransactionRequest.builder()
                .accountId("UNKNOWN_ACCOUNT")
                .amount(new BigDecimal("100.00"))
                .type(Transaction.TransactionType.DEBIT)
                .build();

        TransactionResponse response = transactionService.processTransaction(request);

        assertThat(response.getStatus()).isEqualTo("REJECTED");
        assertThat(response.getMessage()).contains("Account not found");
    }

    @Test
    @DisplayName("Should return balance from Redis cache for known account")
    void shouldReturnBalanceFromRedisCache() {
        when(valueOperations.get(anyString())).thenReturn("5000000");

        BigDecimal balance = transactionService.getBalance("ACC004");

        assertThat(balance).isEqualByComparingTo(new BigDecimal("50000.00"));
        verify(valueOperations, times(1)).get(anyString());
    }

    @Test
    @DisplayName("Should return null for unknown account — no auto-seed")
    void shouldReturnNullForUnknownAccount() {
        when(valueOperations.get(anyString())).thenReturn(null);

        BigDecimal balance = transactionService.getBalance("UNKNOWN");

        assertThat(balance).isNull();
        // Verify we never wrote a default balance (no auto-seed)
        verify(valueOperations, never()).set(anyString(), anyString());
    }

    @Test
    @DisplayName("Should NOT call Kafka when transaction is rejected")
    void shouldNotPublishRejectedTransactionToKafka() {
        when(valueOperations.get(anyString())).thenReturn("1000");
        when(valueOperations.increment(anyString(), anyLong()))
                .thenReturn(-999999L)
                .thenReturn(1L);

        TransactionRequest request = TransactionRequest.builder()
                .accountId("ACC008")
                .amount(new BigDecimal("999999.00"))
                .type(Transaction.TransactionType.DEBIT)
                .build();

        transactionService.processTransaction(request);

        // This verifies kafkaTemplate was truly never called — not just that response says REJECTED
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    @DisplayName("Should NOT call Kafka when account not found")
    void shouldNotPublishWhenAccountNotFound() {
        when(valueOperations.get(anyString())).thenReturn(null);

        TransactionRequest request = TransactionRequest.builder()
                .accountId("GHOST")
                .amount(new BigDecimal("100.00"))
                .type(Transaction.TransactionType.DEBIT)
                .build();

        transactionService.processTransaction(request);

        verify(kafkaTemplate, never()).send(any(), any(), any());
    }
}
