package io.github.thiagoft.log.service;

import io.github.thiagoft.common.service.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        var consumer = new KafkaConsumerService<>(
            Pattern.compile("ECOMMERCE.*"),
            logService.getProperties(),
            logService::consumeOrders
        );
        consumer.run();
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
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return properties;
    }
}
