package io.github.thiagoft.inventory.service;

import io.github.thiagoft.common.service.KafkaConsumerService;
import io.github.thiagoft.common.utils.GsonDeserializer;
import io.github.thiagoft.order.dto.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.UUID;

public class InventoryService {

    public static void main(String[] args) {
        var inventoryService = new InventoryService();
        var consumer = new KafkaConsumerService(
                "ECOMMERCE_NEW_ORDER",
                inventoryService.getProperties(),
                inventoryService::consumeOrders
        );
        consumer.run();
    }

    public void consumeOrders(ConsumerRecord<String, Order> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
        System.out.println("Separating inventory");
    }

    public Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, InventoryService.class.getSimpleName()+"-"+ UUID.randomUUID());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, InventoryService.class.getSimpleName());
        properties.setProperty(GsonDeserializer.CONSUMER_TYPE, Order.class.getName());

        return properties;
    }

}
