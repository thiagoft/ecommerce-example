package io.github.thiagoft.common.utils;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {

    private final Gson gson = new Gson().newBuilder().create();
    private Class<T> type;

    public static final String CONSUMER_TYPE = "CONSUMER_TYPE";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String typeName = String.valueOf(configs.get(CONSUMER_TYPE));

        try {
            this.type = (Class<T>) Class.forName(typeName);
        } catch (ClassNotFoundException classNotFoundException) {
            throw new RuntimeException("Class not found.", classNotFoundException);
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return gson.fromJson(new String(bytes), type);
    }
}
