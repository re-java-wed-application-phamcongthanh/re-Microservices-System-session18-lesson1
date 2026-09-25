package com.shopmart.inventory.saga;

import com.shopmart.inventory.event.KafkaTopics;
import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import com.shopmart.inventory.exception.InsufficientStockException;
import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventorySagaConsumer {

    private final ProductService productService;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "inventory-saga-group")
    public void handleOrderSagaEvent(OrderEvent event) {
        log.info("[InventoryService Saga Consumer] Received event: {}", event);

        if (event.getType() == SagaEventType.ORDER_CREATED) {
            handleOrderCreated(event);
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            handlePaymentFailed(event);
        }
    }

    private void handleOrderCreated(OrderEvent event) {
        try {
            log.info("Saga Step: Reserving stock for orderId={}, productId={}, quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            productService.decreaseStock(event.getProductId(), event.getQuantity());

            OrderEvent reservedEvent = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_RESERVED)
                    .message("Stock reserved successfully")
                    .build();

            kafkaTemplate.send(KafkaTopics.ORDER, reservedEvent);
            log.info("Published INVENTORY_RESERVED for orderId={}", event.getOrderId());
        } catch (InsufficientStockException e) {
            log.error("Saga Step Failed: Insufficient stock for orderId={}: {}", event.getOrderId(), e.getMessage());
            OrderEvent failedEvent = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_FAILED)
                    .message(e.getMessage())
                    .build();

            kafkaTemplate.send(KafkaTopics.ORDER, failedEvent);
        } catch (Exception e) {
            log.error("Saga Step Failed: Unexpected error for orderId={}: {}", event.getOrderId(), e.getMessage());
            OrderEvent failedEvent = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_FAILED)
                    .message("Unexpected inventory error: " + e.getMessage())
                    .build();

            kafkaTemplate.send(KafkaTopics.ORDER, failedEvent);
        }
    }

    private void handlePaymentFailed(OrderEvent event) {
        log.warn("Saga Rollback Triggered: Payment failed for orderId={}. Restoring inventory...", event.getOrderId());
        productService.increaseStock(event.getProductId(), event.getQuantity());
        log.info("Saga Rollback Completed: Restored stock for productId={}, quantity={} for orderId={}",
                event.getProductId(), event.getQuantity(), event.getOrderId());

        OrderEvent releasedEvent = OrderEvent.builder()
                .orderId(event.getOrderId())
                .productId(event.getProductId())
                .quantity(event.getQuantity())
                .amount(event.getAmount())
                .type(SagaEventType.INVENTORY_RELEASED)
                .message("Saga Compensating Rollback: Restored stock of product id=" + event.getProductId())
                .build();

        kafkaTemplate.send(KafkaTopics.ORDER, releasedEvent);
    }
}
