package com.mzq.hello.flink;

import com.mzq.hello.flink.usage.EventTimeUsage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HelloWorldFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EventTimeUsage eventTimeUsage = new EventTimeUsage(streamExecutionEnvironment);
        eventTimeUsage.submitJob();
    }
}