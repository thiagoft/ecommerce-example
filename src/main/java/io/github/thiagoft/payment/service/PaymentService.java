package io.github.thiagoft.payment.service;

import io.github.thiagoft.common.service.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class PaymentService {

    public static void main(String[] args) {
        var paymentService = new PaymentService();
        paymentService.consumeOrders();
    }

    public void consumeOrders() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "PAYMENT_SERVICE");

        var kafkaConsumerService = new KafkaConsumerService(properties);
        kafkaConsumerService.consume("ECOMMERCE_NEW_ORDER");
    }

}
