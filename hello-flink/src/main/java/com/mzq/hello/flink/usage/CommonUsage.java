package com.mzq.hello.flink.usage;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class CommonUsage extends BaseFlinkUsage {

    private static final Logger logger = LoggerFactory.getLogger(CommonUsage.class);

    public CommonUsage(StreamExecutionEnvironment streamExecutionEnvironment) {
        super(streamExecutionEnvironment);
    }

    @Override
    protected String jobName() {
        return CommonUsage.class.getName();
    }

    @Override
    protected void setupStream() throws Exception {
        testParallelism();
    }

    private void testReadFile() {
        getStreamExecutionEnvironment().setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource = getStreamExecutionEnvironment().fromElements(1);
        integerDataStreamSource.addSink(new SinkFunction<Integer>() {
            @Override
            public void invoke(Integer value, Context context) throws Exception {
                URL resource = CommonUsage.class.getClassLoader().getResource("test.properties");
                InputStream inputStream = CommonUsage.class.getClassLoader().getResourceAsStream("test.properties");
                Properties properties = new Properties();
                properties.load(inputStream);
                logger.info("file url is:{},properties is:{}", resource, properties);
            }
        });
    }

    private void testParallelism() throws Exception {
        SingleOutputStreamOperator<Integer> integerDataStreamSource = getStreamExecutionEnvironment().addSource(new SourceFunction<Integer>() {
            private int num = 0;

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (true) {
                    ctx.collect(++num);
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {

            }
        }).slotSharingGroup("slot1");

        SingleOutputStreamOperator<Integer> mapStream = integerDataStreamSource.map(val -> val + 10).setParallelism(2).slotSharingGroup("slot1");
        DataStream<Integer> filterStream = mapStream.filter(value -> value >= 12).setParallelism(3).slotSharingGroup("slot2").rescale();
        DataStreamSink<Integer> integerDataStreamSink = filterStream.print().setParallelism(4).slotSharingGroup("slot2");
    }
}
