package com.mzq.hello.flink.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mzq.hello.domain.WaybillC;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class WaybillcDeserializationSchema implements KafkaDeserializationSchema<WaybillC> {

    private ObjectMapper objectMapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public boolean isEndOfStream(WaybillC nextElement) {
        return false;
    }

    @Override
    public WaybillC deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return objectMapper.readValue(record.value(), WaybillC.class);
    }

    @Override
    public TypeInformation<WaybillC> getProducedType() {
        return TypeInformation.of(WaybillC.class);
    }
}
