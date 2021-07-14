package com.mzq.hello.flink;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mzq.hello.domain.WaybillRouteLink;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class WaybillRouteDeSerializer implements Deserializer<WaybillRouteLink> {

    private final ObjectMapper objectMapper;

    public WaybillRouteDeSerializer() {
        this.objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public WaybillRouteLink deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, WaybillRouteLink.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
