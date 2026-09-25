package com.shopmart.inventory.saga;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import com.shopmart.inventory.service.ProductService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class InventorySagaRollbackTest {

    @Mock
    private ProductService productService;

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @InjectMocks
    private InventorySagaConsumer inventorySagaConsumer;

    @Test
    @DisplayName("Test compensating action: Restores stock when PAYMENT_FAILED event is received")
    void testRollbackInventoryOnPaymentFailure() {
        // Given
        OrderEvent paymentFailedEvent = OrderEvent.builder()
                .orderId(100L)
                .productId(3L)
                .quantity(2)
                .amount(new BigDecimal("84000000"))
                .type(SagaEventType.PAYMENT_FAILED)
                .message("Payment amount exceeds maximum limit")
                .build();

        // When
        inventorySagaConsumer.handleOrderSagaEvent(paymentFailedEvent);

        // Then
        // 1. Verify that increaseStock (compensating action) is called with productId=3 and quantity=2
        verify(productService).increaseStock(3L, 2);

        // 2. Verify that INVENTORY_RELEASED event is published back to Kafka topic
        verify(kafkaTemplate).send(eq(KafkaTopics.ORDER), any(OrderEvent.class));
    }
}
