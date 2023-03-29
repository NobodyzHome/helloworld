package com.mzq.hello.flink.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class RedisSource implements Source<String,RedisSplit,String> {

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<String, RedisSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new RedisSourceReader(readerContext);
    }

    @Override
    public SplitEnumerator<RedisSplit, String> createEnumerator(SplitEnumeratorContext<RedisSplit> enumContext) throws Exception {
        return new RedisSplitEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<RedisSplit, String> restoreEnumerator(SplitEnumeratorContext<RedisSplit> enumContext, String checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<RedisSplit> getSplitSerializer() {
        return new RedisSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<String> getEnumeratorCheckpointSerializer() {
        return null;
    }
}
