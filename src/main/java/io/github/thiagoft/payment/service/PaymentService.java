package io.github.thiagoft.payment.service;

import io.github.thiagoft.common.service.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.UUID;

public class PaymentService {

    public static void main(String[] args) {
        var paymentService = new PaymentService();
        var consumer = new KafkaConsumerService(
                "ECOMMERCE_NEW_ORDER",
                paymentService.getProperties(),
                paymentService::consumeOrders
        );
        consumer.run();
    }

    public void consumeOrders(ConsumerRecord<String,String> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
        System.out.println("Processing payment");
    }

    public Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, PaymentService.class.getSimpleName()+"-"+ UUID.randomUUID());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, PaymentService.class.getSimpleName());

        return properties;
    }

}
