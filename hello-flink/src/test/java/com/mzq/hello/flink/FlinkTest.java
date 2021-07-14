package com.mzq.hello.flink;

import com.mzq.hello.domain.WaybillRouteLink;
import com.mzq.hello.flink.func.WaybillRouteLinkSummaryProcess;
import com.mzq.hello.flink.func.aggregate.WaybillRouteLinkAggregate;
import com.mzq.hello.flink.func.process.WaybillRouteLinkWindowProcess;
import com.mzq.hello.flink.func.source.WaybillRouteLinkSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.time.Duration;

public class FlinkTest {

    @Test
    public void testAggregateWithWindowFunction() throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaybillRouteLink> waybillRouteLinkDataStreamSource = executionEnvironment.addSource(new WaybillRouteLinkSource());
        SingleOutputStreamOperator<Tuple2<String, String>> aggregateStream = waybillRouteLinkDataStreamSource.keyBy(WaybillRouteLink::getWaybillCode).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new WaybillRouteLinkAggregate(), new WaybillRouteLinkWindowProcess());
        aggregateStream.print();

        executionEnvironment.execute();
    }

    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaybillRouteLink> waybillRouteLinkDataStreamSource = streamExecutionEnvironment.addSource(new WaybillRouteLinkSource());
        SingleOutputStreamOperator<WaybillRouteLink> waybillRouteLinkStream = waybillRouteLinkDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(10)));
        SingleOutputStreamOperator<String> process1 = waybillRouteLinkStream.keyBy(WaybillRouteLink::getWaybillCode).window(TumblingEventTimeWindows.of(Time.milliseconds(100))).process(new WaybillRouteLinkSummaryProcess());
        DataStreamSink<String> print = process1.print();
        streamExecutionEnvironment.execute();
    }

    @Test
    public void test1() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource = streamExecutionEnvironment.fromElements(1);
        IterativeStream<Integer> iterateStream = integerDataStreamSource.iterate();
        SingleOutputStreamOperator<Integer> mapStream = iterateStream.map(value -> {
            int v = value + 2;
            System.out.println(v);
            return v;
        });
        SingleOutputStreamOperator<Integer> filterStream = mapStream.filter(value -> value <= 10);
        DataStream<Integer> integerDataStream = iterateStream.closeWith(filterStream);
        integerDataStream.print("test:");
        streamExecutionEnvironment.execute();
    }
}
