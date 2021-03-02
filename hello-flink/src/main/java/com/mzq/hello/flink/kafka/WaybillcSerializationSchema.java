package com.mzq.hello.flink.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mzq.hello.domain.WaybillC;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class WaybillcSerializationSchema implements KafkaSerializationSchema<WaybillC> {

    private ObjectMapper objectMapper;
    private final String topic;

    public WaybillcSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(WaybillC waybillC, @Nullable Long timestamp) {
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(waybillC);
            return new ProducerRecord<>(topic, waybillC.getWaybillCode().getBytes(), bytes);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
