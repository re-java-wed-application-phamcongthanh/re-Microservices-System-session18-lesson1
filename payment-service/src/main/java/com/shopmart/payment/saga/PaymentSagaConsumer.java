package com.shopmart.payment.saga;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.KafkaTopics;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSagaConsumer {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @KafkaListener(topics = KafkaTopics.ORDER, groupId = "payment-saga-group")
    public void handleOrderSagaEvent(OrderEvent event) {
        log.info("[PaymentService Saga Consumer] Received event: {}", event);

        if (event.getType() == SagaEventType.INVENTORY_RESERVED) {
            handleInventoryReserved(event);
        }
    }

    private void handleInventoryReserved(OrderEvent event) {
        log.info("Saga Step: Processing payment for orderId={}, amount={}", event.getOrderId(), event.getAmount());
        try {
            PaymentResponse paymentResponse = paymentService.processPayment(
                    new PaymentRequest(event.getOrderId(), event.getAmount())
            );

            if (paymentResponse.getStatus() == PaymentStatus.SUCCESS) {
                log.info("Saga Step Success: Payment SUCCESS for orderId={}", event.getOrderId());
                OrderEvent completedEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.PAYMENT_COMPLETED)
                        .message("Payment successful")
                        .build();

                kafkaTemplate.send(KafkaTopics.ORDER, completedEvent);
            } else {
                log.error("Saga Step Failed: Payment FAILED for orderId={}: {}", event.getOrderId(), paymentResponse.getMessage());
                OrderEvent failedEvent = OrderEvent.builder()
                        .orderId(event.getOrderId())
                        .productId(event.getProductId())
                        .quantity(event.getQuantity())
                        .amount(event.getAmount())
                        .type(SagaEventType.PAYMENT_FAILED)
                        .message(paymentResponse.getMessage())
                        .build();

                kafkaTemplate.send(KafkaTopics.ORDER, failedEvent);
            }
        } catch (Exception e) {
            log.error("Saga Step Failed: Exception while processing payment for orderId={}: {}", event.getOrderId(), e.getMessage());
            OrderEvent failedEvent = OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.PAYMENT_FAILED)
                    .message(e.getMessage())
                    .build();

            kafkaTemplate.send(KafkaTopics.ORDER, failedEvent);
        }
    }
}
