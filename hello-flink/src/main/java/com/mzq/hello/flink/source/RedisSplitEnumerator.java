package com.mzq.hello.flink.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class RedisSplitEnumerator implements SplitEnumerator<RedisSplit, String> {

    private String[] commands;
    private Queue<RedisSplit> redisSplits;
    private SplitEnumeratorContext<RedisSplit> enumContext;

    private List<Integer> subTasks = new ArrayList<>();

    public RedisSplitEnumerator(SplitEnumeratorContext<RedisSplit> enumContext) {
        this.enumContext = enumContext;
    }

    @Override
    public void start() {
        commands = new String[]{"hello", "world"};
        redisSplits = new ArrayDeque<>(commands.length);
        Stream.of(commands).map(RedisSplit::new).forEach(redisSplits::add);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        int i = subTasks.indexOf(subtaskId);
        RedisSplit split = redisSplits.poll();
        if (Objects.nonNull(split)) {
            enumContext.assignSplit(split, subtaskId);
        } else {
            enumContext.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<RedisSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {
        subTasks.add(subtaskId);
    }

    @Override
    public String snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
