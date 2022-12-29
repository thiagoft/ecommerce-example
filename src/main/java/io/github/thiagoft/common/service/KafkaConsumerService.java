package io.github.thiagoft.common.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerService implements Closeable {

    private KafkaConsumer<String,String> consumer;

    public KafkaConsumerService(Properties properties) {
        this.consumer = getConsumer(properties);
    }

    private KafkaConsumer<String, String> getConsumer(Properties properties) {
        var kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        kafkaProperties.putAll(properties);

        return new KafkaConsumer<>(kafkaProperties);
    }

    public void consume(String topic) {
        this.consumer.subscribe(List.of(topic));
        while(true) {
            var records = this.consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("found " + records.count() + " records");
                for (var record : records) {
                    System.out.println("--------------------------------------------");
                    System.out.println("Message sent - topic: "+record.topic()+" - partition: "+record.partition()+" - offset: "+record.offset()+" - timestamp: "+record.timestamp());
                    System.out.println("Value: "+record.value());
                }
                System.out.println("--------------------------------------------");
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (consumer != null) {
            consumer.close();
        }
    }
}
