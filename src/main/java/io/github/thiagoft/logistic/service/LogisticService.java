package io.github.thiagoft.logistic.service;

import io.github.thiagoft.common.service.ConsumerFunction;
import io.github.thiagoft.common.service.KafkaConsumerService;
import io.github.thiagoft.common.service.KafkaProducerService;
import io.github.thiagoft.common.utils.GsonDeserializer;
import io.github.thiagoft.notification.dto.Notification;
import io.github.thiagoft.notification.service.NotificationService;
import io.github.thiagoft.order.dto.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class LogisticService {

    public static void main(String[] args) {
        var logisticService = new LogisticService();
        var paymentConsumer = new KafkaConsumerService<>(
                "ECOMMERCE_PAYMENT_PROCESSED",
                logisticService.getLogisticProperties(),
                logisticService::paymentConsumer
        );
    }

    private void paymentConsumer(ConsumerRecord<String, Order> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
        System.out.println("Products in transport.");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        finalizedLogisticNotification(record.value());
    }

    private void finalizedLogisticNotification(Order order) {
        try (var notificationProducer = new KafkaProducerService<Notification>(new Properties())) {
            notificationProducer.send(
                    "ECOMMERCE_NOTIFICATION",
                    order.getId().toString(),
                    new Notification(
                            order.getId(),
                            "teste@teste.com",
                            "Product arrived, order: "+ order.getId(),
                            "Your product arrived!"
                    )
            );
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private Properties getLogisticProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, LogisticService.class.getSimpleName()+"-"+ UUID.randomUUID());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogisticService.class.getSimpleName());
        properties.setProperty(GsonDeserializer.CONSUMER_TYPE, Order.class.getName());

        return properties;
    }


}
