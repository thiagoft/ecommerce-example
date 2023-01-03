package io.github.thiagoft.notification.service;

import io.github.thiagoft.common.service.KafkaConsumerService;
import io.github.thiagoft.common.service.SubscribeType;
import io.github.thiagoft.payment.service.PaymentService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.UUID;

public class NotificationService {

    public static void main(String[] args) {
        var notificationService = new NotificationService();
        var consumer = new KafkaConsumerService(
                "ECOMMERCE_NEW_ORDER",
                notificationService.getProperties(),
                notificationService::consumeOrders,
                SubscribeType.COLLECTION_LIST
        );
        consumer.run();
    }

    public void consumeOrders(ConsumerRecord<String,String> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
        System.out.println("Processing Order");
    }

    public Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, NotificationService.class.getSimpleName()+"-"+ UUID.randomUUID());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, NotificationService.class.getSimpleName());

        return properties;
    }
}
