package io.github.thiagoft.inventory.service;

import io.github.thiagoft.common.service.KafkaConsumerService;
import io.github.thiagoft.common.service.KafkaProducerService;
import io.github.thiagoft.common.utils.GsonDeserializer;
import io.github.thiagoft.notification.dto.Notification;
import io.github.thiagoft.order.dto.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class InventoryService {

    public static void main(String[] args) {
        var inventoryService = new InventoryService();
        var consumeOrder = new KafkaConsumerService<>(
            "ECOMMERCE_NEW_ORDER",
            inventoryService.getOrderConsumerProperties(),
            inventoryService::consumeOrders
        );
        consumeOrder.run();

        var consumerPayment = new KafkaConsumerService<>(
            "ECOMMERCE_PAYMENT_PROCESSED",
            inventoryService.getOrderConsumerProperties(),
            inventoryService::consumePaymentsProcessed
        );
        consumerPayment.run();


    }

    public void consumeOrders(ConsumerRecord<String, Order> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
        System.out.println("Separating inventory");

        var order = record.value();
        inventorySeparated(order);
    }

    public void consumePaymentsProcessed(ConsumerRecord<String, Order> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
        System.out.println("Payment confirmed, decreasing stock");

        var order = record.value();
        inventorySeparatedNotification(order);
    }

    public Properties getOrderConsumerProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, InventoryService.class.getSimpleName()+"-"+ UUID.randomUUID());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, InventoryService.class.getSimpleName());
        properties.setProperty(GsonDeserializer.CONSUMER_TYPE, Order.class.getName());

        return properties;
    }

    public void inventorySeparated(Order order) {
        try (var inventorySeparated = new KafkaProducerService<Order>(new Properties())) {
            inventorySeparated.send("ECOMMERCE_INVENTORY_SEPARATED", order.getId().toString(), order);
        } catch (ExecutionException | IOException | InterruptedException e ) {
            throw new RuntimeException(e);
        }
    }

    public void inventorySeparatedNotification(Order order) {
        try (var notificationKafkaProducer = new KafkaProducerService<Notification>(new Properties())) {
            var notification = new Notification(1l, "teste@teste.com","Order number: "+order.getId(), "Product in separation.");
            notificationKafkaProducer.send("ECOMMERCE_NOTIFICATION", notification.getId().toString(), notification);
        } catch (ExecutionException | IOException | InterruptedException e ) {
            throw new RuntimeException(e);
        }
    }
}
