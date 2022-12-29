package io.github.thiagoft.order.service;

import io.github.thiagoft.common.service.KafkaProducerService;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class OrderService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var orderService = new OrderService();
        orderService.createNewOrder();
    }

    public void createNewOrder() throws ExecutionException, InterruptedException {
        var kafkaProducerService = new KafkaProducerService(new Properties());
        kafkaProducerService.send("ECOMMERCE_NEW_ORDER", "123", "456");
    }
}
