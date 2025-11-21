package com.order_api_service.consumer;

import com.order_api_service.dto.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private double totalPriceSum = 0.0;
    private int totalOrderCount = 0;

    @RetryableTopic(
        attempts = "3",
        backoff = @Backoff(delay = 1000, multiplier = 2.0),
        dltStrategy = DltStrategy.FAIL_ON_ERROR, 
        dltTopicSuffix = "-dlq" 
    )

    @KafkaListener(topics = "${app.topics.orders}", groupId = "order-processor-group")
    public void handleOrder(Order order, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Processing order {} from topic {}", order.getOrderId(), topic);

        // synchronized block to ensure thread safety
        synchronized(this) {
            totalPriceSum += order.getPrice();
            totalOrderCount++;
            
            double runningAverage = totalPriceSum / totalOrderCount;
            
            log.info("💰 Price: {}, New Running Average: {}", order.getPrice(), String.format("%.2f", runningAverage));
        }

        // --- SIMULATE TEMPORARY FAILURE ---
        if ("fail-temp".equalsIgnoreCase(order.getProduct().toString())) {
            log.warn("Simulating temporary failure for order {}", order.getOrderId());
            throw new RuntimeException("Temporary database connection issue!");
        }

        // --- SIMULATE PERMANENT FAILURE ---
        if (order.getPrice() < 0) {
            log.error("Permanent failure! Invalid price for order {}.", order.getOrderId());
            throw new IllegalArgumentException("Invalid price: " + order.getPrice());
        }

        log.info("Successfully processed order {}", order.getOrderId());
    }

    @KafkaListener(topics = "${app.topics.orders}-dlq", groupId = "dlq-handler-group")
    public void handleDlt(Order failedOrder, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("!!! DLQ RECEIVED !!! from topic {}: {}", topic, failedOrder);
        
    }
}