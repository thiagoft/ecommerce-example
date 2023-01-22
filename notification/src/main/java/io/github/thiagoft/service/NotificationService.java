package io.github.thiagoft.service;

import io.github.thiagoft.dto.Notification;
import io.github.thiagoft.utils.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.UUID;

public class NotificationService {

    public static void main(String[] args) {
        var notificationService = new NotificationService();
        var consumer = new KafkaConsumerService<>(
                "ECOMMERCE_NOTIFICATION",
                notificationService.getProperties(),
                notificationService::consumeOrders
        );
        consumer.run();
    }

    public void consumeOrders(ConsumerRecord<String, Notification> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
        System.out.println("Processing Order");
    }

    public Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, NotificationService.class.getSimpleName()+"-"+ UUID.randomUUID());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, NotificationService.class.getSimpleName());
        properties.setProperty(GsonDeserializer.CONSUMER_TYPE, Notification.class.getName());

        return properties;
    }
}
