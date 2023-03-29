package com.mzq.hello.flink.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class RedisSourceReader implements SourceReader<String, RedisSplit> {

    private List<RedisSplit> splits;

    private SourceReaderContext readerContext;

    private boolean end = false;

    public RedisSourceReader(SourceReaderContext readerContext) {
        this.readerContext = readerContext;
        splits = new ArrayList<>();
    }

    @Override
    public void start() {
        readerContext.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
        if (!end) {
            if (Objects.isNull(splits) || splits.isEmpty()) {
                return InputStatus.NOTHING_AVAILABLE;
            }

            RedisSplit redisSplit = splits.remove(0);
            output.collect(redisSplit.splitId());
            readerContext.sendSplitRequest();
            return InputStatus.MORE_AVAILABLE;
        }
        return InputStatus.END_OF_INPUT;
    }


    @Override
    public List<RedisSplit> snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<RedisSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        end = true;
    }

    @Override
    public void close() throws Exception {

    }
}
