package io.github.thiagoft.order.service;

import io.github.thiagoft.common.service.KafkaProducerService;
import io.github.thiagoft.notification.dto.Notification;
import io.github.thiagoft.order.dto.Order;
import io.github.thiagoft.order.dto.Product;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class OrderService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var orderService = new OrderService();

        var order = new Order(123l,
                10.0,
                List.of(new Product(1l,"Teste", 10.0)));
        orderService.createNewOrder(order);
    }

    public void createNewOrder(Order order) throws ExecutionException, InterruptedException {
        try (var orderKafkaProducer = new KafkaProducerService<Order>(new Properties())) {
            try(var notificationKafkaProducer = new KafkaProducerService<Notification>(new Properties())) {
                orderKafkaProducer.send("ECOMMERCE_NEW_ORDER", order.getId().toString(), order);

                var notification = new Notification(1l, "teste@teste.com","Order number: "+order.getId(), "Processing your order.");
                notificationKafkaProducer.send("ECOMMERCE_NOTIFICATION", notification.getId().toString(), notification);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
