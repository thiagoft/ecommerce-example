package io.github.thiagoft.common.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaConsumerService implements Closeable {

    private final KafkaConsumer<String,String> consumer;
    private final ConsumerFunction consumerFunction;

    public KafkaConsumerService(String topic, Properties properties, ConsumerFunction consumerFunction, SubscribeType subscribeType) {
        this.consumerFunction = consumerFunction;
        this.consumer = new KafkaConsumer<>(getProperties(properties));

        switch (subscribeType) {
            case COLLECTION_LIST -> this.consumer.subscribe(Collections.singletonList(topic));
            case PATTERN_MATCHING -> this.consumer.subscribe(Pattern.compile(topic+".*"));
        }

    }

    private Properties getProperties(Properties properties) {
        var kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        kafkaProperties.putAll(properties);

        return kafkaProperties;
    }

    public void run() {

        while(true) {
            var records = this.consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("found " + records.count() + " records");
                for (var record : records) {
                    this.consumerFunction.consume(record);
                }
                System.out.println("--------------------------------------------");
            }
        }
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }
}
