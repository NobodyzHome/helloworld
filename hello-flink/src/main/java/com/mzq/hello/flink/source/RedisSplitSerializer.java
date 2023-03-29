package com.mzq.hello.flink.source;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class RedisSplitSerializer implements SimpleVersionedSerializer<RedisSplit> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(RedisSplit obj) throws IOException {
        return SerializationUtils.serialize(obj);
    }

    @Override
    public RedisSplit deserialize(int version, byte[] serialized) throws IOException {
        return SerializationUtils.deserialize(serialized);
    }
}
