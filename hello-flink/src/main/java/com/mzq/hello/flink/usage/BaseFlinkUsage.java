package com.mzq.hello.flink.usage;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

abstract class BaseFlinkUsage {

    private final StreamExecutionEnvironment streamExecutionEnvironment;

    public BaseFlinkUsage(StreamExecutionEnvironment streamExecutionEnvironment) {
        this.streamExecutionEnvironment = streamExecutionEnvironment;
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return streamExecutionEnvironment;
    }

    public void submitJob() throws Exception{
        setupStream();
        getStreamExecutionEnvironment().execute(jobName());
    }

    protected abstract String jobName();

    protected abstract void setupStream() throws Exception;
}
