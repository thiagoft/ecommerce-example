package io.github.thiagoft.payment.service;

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

public class PaymentService {

    public static void main(String[] args) {
        var paymentService = new PaymentService();
        var consumer = new KafkaConsumerService<>(
                "ECOMMERCE_NEW_ORDER",
                paymentService.getProperties(),
                paymentService::consumeOrders
        );
        consumer.run();
    }

    public void consumeOrders(ConsumerRecord<String,Order> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
        System.out.println("Processing payment");

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        paymentProcessed(record.value());
        paymentProcessedNotification(record.value());
    }

    public void paymentProcessed(Order order) {
        try (var paymentProcessed = new KafkaProducerService<Order>(new Properties())) {
            paymentProcessed.send("ECOMMERCE_PAYMENT_PROCESSED", order.getId().toString(), order);
        } catch (ExecutionException | IOException | InterruptedException e ) {
            throw new RuntimeException(e);
        }
    }

    public void paymentProcessedNotification(Order order) {
        try (var notificationKafkaProducer = new KafkaProducerService<Notification>(new Properties())) {
            var notification = new Notification(1l, "teste@teste.com","Order number: "+order.getId(), "Payment processed.");
            notificationKafkaProducer.send("ECOMMERCE_NOTIFICATION", notification.getId().toString(), notification);
        } catch (ExecutionException | IOException | InterruptedException e ) {
            throw new RuntimeException(e);
        }
    }

    public Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, PaymentService.class.getSimpleName()+"-"+ UUID.randomUUID());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, PaymentService.class.getSimpleName());
        properties.setProperty(GsonDeserializer.CONSUMER_TYPE,Order.class.getName());

        return properties;
    }

}
