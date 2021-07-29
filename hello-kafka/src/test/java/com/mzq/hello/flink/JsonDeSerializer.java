package com.mzq.hello.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

abstract class JsonDeSerializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;

    public JsonDeSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        T value = null;
        try {
            value = objectMapper.readValue(data, getTargetClass());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return value;
    }

    abstract Class<T> getTargetClass();
}
