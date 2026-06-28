package com.surgeflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.surgeflow.model.Transaction;
import com.surgeflow.model.TransactionRequest;
import com.surgeflow.model.TransactionResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionServiceTest {
    @Mock private RedisTemplate<String, String> redisTemplate;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private ObjectMapper objectMapper;
    @Mock private ValueOperations<String, String> valueOperations;
    @InjectMocks private TransactionService transactionService;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void shouldApproveDebit() throws Exception {
        when(valueOperations.setIfAbsent(anyString(), anyString())).thenReturn(true);
        when(valueOperations.increment(anyString(), anyLong())).thenReturn(9950000L);
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doReturn(null).when(kafkaTemplate).send(any(), any(), any());
        TransactionRequest req = TransactionRequest.builder()
            .accountId("ACC001").amount(new BigDecimal("500"))
            .type(Transaction.TransactionType.DEBIT).build();
        assertThat(transactionService.processTransaction(req).getStatus()).isEqualTo("APPROVED");
    }

    @Test
    void shouldRejectDebit() {
        when(valueOperations.setIfAbsent(anyString(), anyString())).thenReturn(true);
        when(valueOperations.increment(anyString(), anyLong()))
            .thenReturn(-99999900L).thenReturn(1L);
        TransactionRequest req = TransactionRequest.builder()
            .accountId("ACC002").amount(new BigDecimal("999999"))
            .type(Transaction.TransactionType.DEBIT).build();
        assertThat(transactionService.processTransaction(req).getStatus()).isEqualTo("REJECTED");
    }

    @Test
    void shouldReturnBalance() {
        when(valueOperations.get(anyString())).thenReturn("5000000");
        assertThat(transactionService.getBalance("ACC001"))
            .isEqualByComparingTo(new BigDecimal("50000.00"));
    }

    @Test
    void shouldNotPublishRejectedToKafka() {
        when(valueOperations.setIfAbsent(anyString(), anyString())).thenReturn(true);
        when(valueOperations.increment(anyString(), anyLong()))
            .thenReturn(-999999L).thenReturn(1L);
        TransactionRequest req = TransactionRequest.builder()
            .accountId("ACC003").amount(new BigDecimal("999999"))
            .type(Transaction.TransactionType.DEBIT).build();
        transactionService.processTransaction(req);
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }
}
