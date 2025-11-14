package com.order_api_service.controller;

import com.order_api_service.dto.Order;
import com.order_api_service.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class OrderProducerController {

    @Autowired
    private OrderProducer orderProducer;

    @PostMapping("/orders")
    public String sendMessage(@RequestBody Order order) {
        orderProducer.sendMessage(order);
        return "Order sent successfully with ID: " + order;
    }
}