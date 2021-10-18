package com.mzq.hello.flink.usage;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
        testReadFile();
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
}
