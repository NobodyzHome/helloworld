package com.mzq.hello.flink.sql.connector.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class RedisSink implements Sink<RowData> {

    private final String url;
    private final Integer[] metaPositions;

    public RedisSink(String url, Integer[] metaPositions) {
        this.url = url;
        this.metaPositions = metaPositions;
    }

    public String getUrl() {
        return url;
    }

    public Integer[] getMetaPositions() {
        return metaPositions;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        return new RedisSinkWriter(getUrl(), getMetaPositions(), context.getProcessingTimeService());
    }
}
