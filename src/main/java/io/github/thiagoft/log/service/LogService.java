package io.github.thiagoft.log.service;

import io.github.thiagoft.common.service.KafkaConsumerService;
import io.github.thiagoft.common.service.SubscribeType;
import io.github.thiagoft.notification.service.NotificationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.UUID;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        var consumer = new KafkaConsumerService(
                "ECOMMERCE_NEW_ORDER",
                logService.getProperties(),
                logService::consumeOrders,
                SubscribeType.PATTERN_MATCHING
        );
    }

    public void consumeOrders(ConsumerRecord<String,String> record) {
        System.out.println("--------------------------------------------");
        System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
        System.out.println("Value: "+record.value());
    }

    public Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, LogService.class.getSimpleName()+"-"+ UUID.randomUUID());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());

        return properties;
    }
}
