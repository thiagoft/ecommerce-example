package io.github.thiagoft.common.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {
    void consume(ConsumerRecord<String,T> consumerRecord);
}
