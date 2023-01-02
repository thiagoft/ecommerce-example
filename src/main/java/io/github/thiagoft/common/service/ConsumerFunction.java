package io.github.thiagoft.common.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction {
    public void consume(ConsumerRecord<String,String> consumerRecord);
}
