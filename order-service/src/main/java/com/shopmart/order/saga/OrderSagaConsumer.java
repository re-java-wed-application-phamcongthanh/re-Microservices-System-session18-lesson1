package com.shopmart.order.saga;

import com.shopmart.order.event.KafkaTopics;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaConsumer {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "order-saga-group")
    public void handleOrderSagaEvent(OrderEvent event) {
        log.info("[OrderService Saga Consumer] Received event: {}", event);

        if (event.getType() == SagaEventType.PAYMENT_COMPLETED) {
            log.info("Saga Success: Order id={} completed payment. Completing order...", event.getOrderId());
            orderService.completeOrder(event.getOrderId());
        } else if (event.getType() == SagaEventType.INVENTORY_FAILED) {
            log.warn("Saga Failed: Inventory reservation failed for order id={}. Reason: {}", event.getOrderId(), event.getMessage());
            orderService.cancelOrder(event.getOrderId(), event.getMessage());
        } else if (event.getType() == SagaEventType.PAYMENT_FAILED) {
            log.warn("Saga Failed: Payment failed for order id={}. Reason: {}", event.getOrderId(), event.getMessage());
            orderService.cancelOrder(event.getOrderId(), event.getMessage());
        }
    }
}
