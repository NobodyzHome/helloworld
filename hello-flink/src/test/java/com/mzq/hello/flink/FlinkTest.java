package com.mzq.hello.flink;

import com.mzq.hello.domain.WaybillRouteLink;
import com.mzq.hello.flink.func.aggregate.WaybillRouteLinkAggregate;
import com.mzq.hello.flink.func.process.WaybillRouteLinkWindowProcess;
import com.mzq.hello.flink.func.source.WaybillRouteLinkSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

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
}
