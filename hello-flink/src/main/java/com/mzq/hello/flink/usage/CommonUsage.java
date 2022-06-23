package com.mzq.hello.flink.usage;

import com.mzq.hello.domain.ItemDeal;
import com.mzq.hello.flink.func.broadcast.BroadcastProcessFun;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.util.Objects;
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
//        testParallelism();
        testBroadcast();
//        testKeyedCoProcess();
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

    public void testBroadcast() {
        getStreamExecutionEnvironment().setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> priceStream = getStreamExecutionEnvironment().fromElements(new Tuple2<>("apple", 20), new Tuple2<>("banana", 30), new Tuple2<>("orange", 10));
        // 1.创建broadcastStream
        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("test", String.class, Integer.class);
        BroadcastStream<Tuple2<String, Integer>> broadcastStream = priceStream.broadcast(mapStateDescriptor);
        // 2.创建非broadcastStream
        DataStreamSource<ItemDeal> itemDealSource = getStreamExecutionEnvironment().fromElements(new ItemDeal("orange", 11.3d), new ItemDeal("orange", 23.75d)
                , new ItemDeal("orange", 11.63d), new ItemDeal("banana", 3.5d), new ItemDeal("apple", 4d), new ItemDeal("apple", 6.2d)
                , new ItemDeal("banana", 7d));
        // 3.broadcastStream和非broadcastStream connect，然后使用BroadcastProcessFunction处理
        SingleOutputStreamOperator<Tuple2<String, Double>> itemDealCountStream = itemDealSource.connect(broadcastStream).process(new BroadcastProcessFun(mapStateDescriptor));
        itemDealCountStream.print().setParallelism(2);
    }

    public void testKeyedCoProcess() {
        getStreamExecutionEnvironment().setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> priceStream = getStreamExecutionEnvironment().fromElements(new Tuple2<>("apple", 20), new Tuple2<>("banana", 30), new Tuple2<>("orange", 10));
        DataStreamSource<ItemDeal> itemDealSource = getStreamExecutionEnvironment().fromElements(new ItemDeal("orange", 11.3d), new ItemDeal("orange", 23.75d)
                , new ItemDeal("orange", 11.63d), new ItemDeal("banana", 3.5d), new ItemDeal("apple", 4d), new ItemDeal("apple", 6.2d)
                , new ItemDeal("banana", 7d));

        SingleOutputStreamOperator<Tuple2<String, Double>> itemDealCntStream = priceStream.connect(itemDealSource).keyBy(tuple -> tuple._1, ItemDeal::getItemName).process(new KeyedCoProcessFunction<String, Tuple2<String, Integer>, ItemDeal, Tuple2<String, Double>>() {

            private ValueState<Integer> priceState;

            private ListState<ItemDeal> itemDealListState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("price", Integer.class));
                itemDealListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemDeal>("item", ItemDeal.class));
            }


            @Override
            public void processElement1(Tuple2<String, Integer> value, KeyedCoProcessFunction<String, Tuple2<String, Integer>, ItemDeal, Tuple2<String, Double>>.Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                priceState.update(value._2);
            }

            @Override
            public void processElement2(ItemDeal value, KeyedCoProcessFunction<String, Tuple2<String, Integer>, ItemDeal, Tuple2<String, Double>>.Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                Integer price = priceState.value();
                if (Objects.nonNull(price)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), new BigDecimal(price).multiply(BigDecimal.valueOf(value.getDealCount())).setScale(2, RoundingMode.HALF_UP).doubleValue()));

                    boolean hasHistory = false;
                    for (ItemDeal history : itemDealListState.get()) {
                        out.collect(new Tuple2<>(ctx.getCurrentKey(), new BigDecimal(price).multiply(BigDecimal.valueOf(history.getDealCount())).setScale(2, RoundingMode.HALF_UP).doubleValue()));
                        hasHistory = true;
                    }
                    if (hasHistory) {
                        itemDealListState.clear();
                    }
                } else {
                    itemDealListState.add(value);
                }
            }
        });
        itemDealCntStream.print();
    }
}
