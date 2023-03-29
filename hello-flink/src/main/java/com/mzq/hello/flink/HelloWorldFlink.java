package com.mzq.hello.flink;

import com.mzq.hello.flink.source.RedisSource;
import com.mzq.hello.flink.usage.CommonUsage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class HelloWorldFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
//        CommonUsage commonUsage = new CommonUsage(streamExecutionEnvironment);
//        commonUsage.submitJob();

        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.fromSource(new RedisSource(), WatermarkStrategy.noWatermarks(),"redis-source", TypeInformation.of(String.class));
        stringDataStreamSource.sinkTo(new PrintSink<>(true));
        streamExecutionEnvironment.execute();
    }
}