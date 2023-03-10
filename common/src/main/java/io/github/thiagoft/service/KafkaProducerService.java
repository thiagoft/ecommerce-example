package io.github.thiagoft.service;

import io.github.thiagoft.utils.GsonSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerService<T> implements Closeable {

    private KafkaProducer<String,T> producer;

    public KafkaProducerService(Properties properties) {
        this.producer = getProducer(properties);
    }

    private KafkaProducer<String,T> getProducer(Properties properties) {
        var kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        kafkaProperties.putAll(properties);

        return new KafkaProducer<>(kafkaProperties);
    }

    public void send(String topic, String key, T obj) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, obj);
        Callback callback = (data, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }
            System.out.println("Message sent - topic: "+data.topic()+" - partition: "+data.partition()+" - offset: "+data.offset()+" - timestamp: "+data.timestamp());
        };

        this.producer.send(record,callback).get();
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }
}
