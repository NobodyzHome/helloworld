package com.mzq.hello.flink.usage;

import com.mzq.hello.domain.WaybillC;
import com.mzq.hello.domain.WaybillRouteLink;
import com.mzq.hello.flink.func.source.WaybillCSource;
import com.mzq.hello.flink.func.source.WaybillRouteLinkSource;
import com.mzq.hello.flink.kafka.WaybillcDeserializationSchema;
import com.mzq.hello.flink.kafka.WaybillcSerializationSchema;
import com.mzq.hello.util.GenerateDomainUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Properties;
import java.util.StringJoiner;

/**
 * 【元素是几点来的？】
 * 在flink的job中，一些算子是对时间不敏感的，例如map算子，它只需要对输入的元素进行转换，并不关心元素是几点来的（也就是元素的时间戳是什么）。但有的算子是时间敏感的，例如window算子只有知道一个元素的时间戳（也就是知道这个元素是几点来的），才能确定为这个元素指派的window的start和end是多少。
 * 当一个job使用processing_time时，一个元素的时间戳是由这个元素何时被算子接收所决定的。
 * 当一个job使用event_time时，一个元素的时间戳由这个元素自己决定，例如一个元素上的update_time字段的值作为这个元素的时间戳。
 * <p>
 * 【现在几点了？】
 * 在我们确定好一个算子是几点来的之后，我们要关心的下一个话题就是【现在几点了？】，例如我知道一个window是21:30进行fire，但是我得知道现在几点了，才能决定它现在是否需要fire。在processing_time中，这个问题非常简单，例如一个window的end是21:30，那么很自然地只要当系统时间到达21:30就会fire这个window。
 * 但在event_time中，我们现在只知道window的end是21:30（根据元素的时间戳计算出来的），但我们不知道现在几点了。因此就引出一个下一个话题，watermark。watermark用于告诉算子，当前时间几点了。例如一个window算子在接收到21:30的时间戳后，那么它就会把所有end在21:30之前的window都fire了，
 * 然后把watermark发送给下游。watermark也是由assignTimestampsAndWatermarks算子产生，然后发送给下游算子。下游算子的processWatermark()方法用于处理上游发来的watermark。
 * <p>
 * 【使用时间的算子】
 * 现在我们知道每个元素是几点来的，以及现在几点了，还有一件事就是知道哪些算子需要使用时间。并不是所有算子的实现都关心时间，例如StreamFilter，它在处理元素时就是进行简单的过滤，它在处理接收到的watermark时，就是简单的把watermark【广播】到下游的所有算子上。
 * 而WindowOperator则很强烈的需要使用时间，它在处理元素时，需要获取元素的时间戳，传给WindowAssigner获取为这个元素指派的window。此后，还会让Trigger根据window的end和接收到的watermark进行比较，如果window end < watermark，则让这个window fire。
 * 因此WindowOperator既需要使用时间戳，也需要使用watermark。
 * <p>
 * 为什么要使用event_time？
 * 我们可以看到，在processing_time中，决定元素几点来的以及决定现在几点了都是使用的系统当前时间。我们知道，在flink集群的构成中，分布了很多的taskManager，在理想状态下，这些机器的时钟都是统一的，也就是这些机器的系统时间都是一致。但当一台机器的时钟同步失效后会产生什么作用？假设window算子在产生window时，
 * taskManager的系统时间被错误地调整到了22:00，那么它产生的window的end有可能是22:00。假设此时这个tm的时间又被修复了，修复回13:00。那么它之前创建的window，需要延迟9个小时才能被fire。
 * 但使用event_time就没有这个问题，因为它获取元素的时间戳永远是根据元素来决定的，而不是根据tm的系统时间，即使tm的时间出现了错误，系统在event_time下依旧可以正确运行。因此使用event_time的主要目的是为了摆脱机器的时钟不正确时，导致flink任务处理有可能也不正确了的问题。
 * <p>
 * 注意：在flink1.12以后，flink默认使用的是event_time。
 *
 * @author maziqiang
 */
public class EventTimeUsage extends BaseFlinkUsage {

    public EventTimeUsage(StreamExecutionEnvironment streamExecutionEnvironment) {
        super(streamExecutionEnvironment);
    }

    @Override
    protected String jobName() {
        return EventTimeUsage.class.getName();
    }

    @Override
    protected void setupStream() throws Exception {
//        basicUseWithAssignTimestampsAndWatermarksOperator();
//        multipleInputWatermark();
//        basicUseWithSource();
//        lateUsage();
//        idleUsage();
        basicUseWithKafka();
    }

    private void basicUseWithSource() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用event time的第一种方式是在source算子产生数据时，就使用SourceContext.collectWithTimestamp下发元素和它的时间戳。以及使用就使用SourceContext.emitWatermark来定期发送watermark。
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new RichSourceFunction<WaybillC>() {
            private boolean isRunning = false;
            private IntCounter counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                isRunning = true;
                counter = getRuntimeContext().getIntCounter("counter");
            }

            @Override
            public void run(SourceContext<WaybillC> ctx) throws Exception {
                while (isRunning) {
                    WaybillC waybillC = new WaybillC();
                    waybillC.setWaybillCode(GenerateDomainUtils.generateOrderCode("JD"));
                    waybillC.setWaybillSign(GenerateDomainUtils.generateSign());
                    waybillC.setSiteCode(GenerateDomainUtils.generateOrderCode("site"));
                    waybillC.setSiteName(waybillC.getSiteCode() + "站点");
                    waybillC.setTimeStamp(System.currentTimeMillis());

                    ctx.collectWithTimestamp(waybillC, waybillC.getTimeStamp());
                    counter.add(1);
                    Integer count = counter.getLocalValue();
                    if (count % 5 == 0) {
                        ctx.emitWatermark(new Watermark(waybillC.getTimeStamp() - 5000));
                    }

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).setParallelism(1);

        // 算子partition的方式应该是不影响watermark传播。在这个例子里，我让source里的数据只发给下游算子里的第一个实例，实际运行结果是：数据确实只下发给下游算子的第一个实例里了，但是下游算子的两个实例依然都接收到了watermark。
        // 因此，上游算子会把watermark传给下游算子的所有实例。无论上游算子和下游算子的partition方式是什么。
        // Emits a Watermark from an operator. This watermark is 【broadcast】 to all downstream operators.
        SingleOutputStreamOperator<Tuple2<String, WaybillC>> tuple2Stream = waybillCDataStreamSource.partitionCustom(new Partitioner<WaybillC>() {
            @Override
            public int partition(WaybillC key, int numPartitions) {
                return 0;
            }
        }, waybillC -> waybillC).map(waybillc -> Tuple2.of(waybillc.getWaybillCode(), waybillc)).returns(Types.TUPLE(Types.STRING, Types.GENERIC(WaybillC.class))).setParallelism(2);
        SingleOutputStreamOperator<String> stringStream = tuple2Stream.keyBy(tuple2 -> tuple2.f0).window(TumblingEventTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<Tuple2<String, WaybillC>, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple2<String, WaybillC>> elements, Collector<String> out) throws Exception {
                elements.forEach(entry -> out.collect(entry.f1.toString()));
            }
        }).setParallelism(3);

        DataStreamSink<String> writeSink = stringStream.writeAsText("/my-files/test", FileSystem.WriteMode.OVERWRITE).setParallelism(4);
        streamExecutionEnvironment.execute();
    }

    private void basicUseWithAssignTimestampsAndWatermarksOperator() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = getStreamExecutionEnvironment();
        DataStreamSource<WaybillRouteLink> waybillRouteLinkDataStreamSource = streamExecutionEnvironment.addSource(new WaybillRouteLinkSource()).setParallelism(1);
         /*
            assignTimestampsAndWatermarks算子用于给上游发来的算子赋值时间戳以及发送watermark
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))产生的watermark是用当前算子接到的最大的时间戳减去3秒，来生成watermark的值。例如当前算子接收到的元素的最大时间戳是13:00，那么它产生的watermark是12:59:57
            注意：assignTimestampsAndWatermarks算子会把watermark传递给下游算子的每个实例，不论上游算子和下游算子的partition是什么。在这里，assignTimestampsAndWatermarks算子和下游map算子的partition是custom，那么它只会把数据发送到map算子的第一个实例中。但是它会把产生的watermark
                 发送给map算子的所有实例。
         */
        SingleOutputStreamOperator<WaybillRouteLink> waybillRouteLinkWithWatermarkStream = waybillRouteLinkDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))).setParallelism(2);
        // 如果算子的上游只有一个输入流，那么这个算子在处理watermark时，就是直接把watermark发送给下游算子
        SingleOutputStreamOperator<String> stringStream = waybillRouteLinkWithWatermarkStream.partitionCustom(new Partitioner<WaybillRouteLink>() {
            @Override
            public int partition(WaybillRouteLink key, int numPartitions) {
                return 0;
            }


        }, waybill -> waybill).map(Object::toString).setParallelism(3);
        DataStreamSink<String> stringSink = stringStream.print();
    }

    public void basicUseWithKafka() {
        StreamExecutionEnvironment streamExecutionEnvironment = getStreamExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        streamExecutionEnvironment.disableOperatorChaining();

        DataStreamSource<WaybillC> waybillRouteLinkDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
        FlinkKafkaProducer<WaybillC> flinkKafkaProducer = new FlinkKafkaProducer<>("waybill-c", new WaybillcSerializationSchema("waybill-c"), producerConfig, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        waybillRouteLinkDataStreamSource.addSink(flinkKafkaProducer);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        FlinkKafkaConsumer<WaybillC> flinkKafkaConsumer = new FlinkKafkaConsumer<>("waybill-c", new WaybillcDeserializationSchema(), consumerConfig);
        /*
            上面我们说了，可以在source方法的实现中使用ctx.collectWithTimestamp方法来下发元素以及它的时间戳，并且也可以用ctx.emitWatermark来发送watermark。
            这种情况针对我们自己实现的SourceFunction是可以的，但对于已经给好实现的FlinkKafkaConsumer，我们能不能在数据源头就自动下发元素的时间戳和watermark呢。
            省的我们再增加一个TimestampsAndWatermarksOperator算子。
            答案是可以的，我们可以在FlinkKafkaConsumer中使用assignTimestampsAndWatermarks方法来设置WatermarkStrategy，这样，数据从kafka读出来以后就带着时间戳，并且
            也可以定期下发watermark。

            给FlinkKafkaConsumer设置WatermarkStrategy还有一个好处是，如果一个Source实例获取到了一个topic的多个分区，我们知道每个分区都会有元素以及元素对应的时间戳，代表每个分区产生的watermark是不一样的，
            FlinkKafkaConsumer内部会获取每一个分区产生watermark，然后选出最小的那个watermark发现给下游。这个效果是在FlinkKafkaConsumer后面跟一个TimestampsAndWatermarksOperator算子所达不到的。
         */
        WatermarkStrategy<WaybillC> watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1));
        watermarkStrategy = watermarkStrategy.withTimestampAssigner((waybillC, timestmap) -> waybillC.getTimeStamp());
        flinkKafkaConsumer.assignTimestampsAndWatermarks(watermarkStrategy);

        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(flinkKafkaConsumer).setParallelism(3);
        SingleOutputStreamOperator<String> stringStream = waybillCDataStreamSource.map(WaybillC::toString).returns(String.class);
        stringStream.print("waybill-c");
    }

    private void multipleInputWatermark() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = getStreamExecutionEnvironment();
        DataStreamSource<WaybillRouteLink> waybillRouteLinkDataStreamSource = streamExecutionEnvironment.addSource(new WaybillRouteLinkSource()).setParallelism(1);
        SingleOutputStreamOperator<WaybillRouteLink> waybillRouteLinkStream = waybillRouteLinkDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))).setParallelism(2);

        DataStreamSource<String> tuple2DataStreamSource = streamExecutionEnvironment.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    Tuple2<String, Long> tuple = Tuple2.of(GenerateDomainUtils.generateOrderCode("JD"), ZonedDateTime.now().plusMinutes(RandomUtils.nextLong(1, 11)).toInstant().toEpochMilli());
                    ctx.collectWithTimestamp(tuple.f0, tuple.f1);
                    Thread.sleep(3000);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(1);

        SingleOutputStreamOperator<String> tuple2SingleOutputStreamOperator = tuple2DataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))).setParallelism(2);
         /*
            当一个算子的上游输入有两个数据流时，那么它处理上游这两个watermark时，就是发送这两个watermark里最小的那个
            对于只有一个数据输入来源的StreamOperator的具体类，它的父接口是OneInputStreamOperator。OneInputStreamOperator接口又继承了Input接口，Input接口提供了处理单个数据源来的数据的方法。即processElement方法用于处理上游发来的数据，processWatermark用于处理上游发来的watermark
            OneInputStreamOperator处理watermark的逻辑相对简单，就是把接到的watermark存储起来，然后到时间该执行的watermark执行了，最后发送给下游算子。
            而对于有两个数据输入来源的StreamOperator的具体类，它的父接口是TwoInputStreamOperator,它提供了processElement1、processElement2方法用于分别处理数据源1和2的数据，processWatermark1、processWatermark2方法用于分别处理数据源1和2发送的watermark。
            这里就遇到问题了，输入的两个数据源发来不同的watermark，可是TwoInputStreamOperator只能向下游发一个watermark，那么应该发哪个呢？TwoInputStreamOperator的实现是取上游这两个数据源发来的watermark中最小的，把它发到下游。
            这也就是代表了，只有上游两个数据源的watermark都到齐了，TwoInputStreamOperator才会发这两个watermark里最小的那个。造成的一个问题就是如果其中一个数据源迟迟不发watermark的话，那么TwoInputStreamOperator一直不会把watermark发给下游算子。我们可以使用WatermarkStrategy.withIdleness来解决这个问题
         */
        SingleOutputStreamOperator<String> packageCodeJoinStream = waybillRouteLinkStream.connect(tuple2SingleOutputStreamOperator).keyBy(WaybillRouteLink::getWaybillCode, value -> value).process(new CoProcessFunction<WaybillRouteLink, String, String>() {
            @Override
            public void processElement1(WaybillRouteLink value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value.getPackageCode());
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }
        }).setParallelism(3);

        SingleOutputStreamOperator<String> stringStream = packageCodeJoinStream.keyBy(value -> value).window(TumblingEventTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                StringJoiner joiner = new StringJoiner(",");
                elements.forEach(joiner::add);
                out.collect(joiner.toString());
            }
        }).setParallelism(4);

        DataStreamSink<String> print = stringStream.print().setParallelism(5);
        streamExecutionEnvironment.execute();
    }

    public void lateUsage() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = getStreamExecutionEnvironment();
        ExecutionConfig config = streamExecutionEnvironment.getConfig();
        config.setAutoWatermarkInterval(1);

        DataStreamSource<Tuple2<String, Long>> source = streamExecutionEnvironment.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                long now = System.currentTimeMillis();
                for (Tuple2<String, Long> tuple : Arrays.asList(Tuple2.of("A", now), Tuple2.of("B", now - 24000), Tuple2.of("C", now + 1000))) {
                    ctx.collect(tuple);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        WatermarkStrategy<Tuple2<String, Long>> watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();
        watermarkStrategy = watermarkStrategy.withTimestampAssigner((element, recordTimestamp) -> element.f1);
         /*
            TimestampsAndWatermarksOperator会永不间断地注册一个timer，来调用WatermarkGenerator的onPeriodicEmit方法。注册timer的间隔就是根据streamExecutionEnvironment.getConfig().setAutoWatermarkInterval()来设置的
            如果AutoWatermarkInterval越长，举个夸张点的例子是3秒，这样0～3秒间WindowOperator由于没有接收到watermark，这时WindowOperator默认的watermark则是Long.MIN_VALUE，因此在这段期间它接收的所有数据的window的end，应该是都大于Long.MIN_VALUE，所以这时它接收到的数据都会被处理。
            而3秒后，WindowOperator收到了TimestampsAndWatermarksOperator传来的watermark，这时再有比较老的timestamp的元素传入到WindowOperator，WindowOperator可能就会认定他已经迟到了。
            也就是说，在TimestampsAndWatermarksOperator中，每个元素的时间戳是随着TimestampsAndWatermarksOperator的processElement方法随着元素直接下发给下游算子的，但是watermark是根据AutoWatermarkInterval定期下发给下游算子的。
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2WithWatermarkStream = source.assignTimestampsAndWatermarks(watermarkStrategy).setParallelism(1);

        OutputTag<Tuple2<String, Long>> lateTag = new OutputTag<>("test", Types.TUPLE(Types.STRING, Types.LONG));
        // 判断元素迟到主要是针对WindowOperator来说的，当一个元素到来时，WindowOperator会使用WindowAssigner为这个元素分配window，如果分配的window的end比WindowOperator接到的watermark要小的话，那么就认为这个元素迟到了。如果不设置allowedLateness，那么这个迟到的元素就会
        // 被WindowOperator舍弃。如果设置了allowedLateness的话，会再给这个元素一次机会，如果window end + allowedLateness > watermark的话，这个元素还是会被处理，但是如果这样watermark小的话，该元素依旧会因为迟到被舍弃。
        SingleOutputStreamOperator<String> stringStream = tuple2WithWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3))).allowedLateness(Time.seconds(2)).sideOutputLateData(lateTag)
                .process(new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        StringJoiner stringJoiner = new StringJoiner(",");
                        elements.forEach(tuple -> stringJoiner.add(tuple.f0));
                        out.collect(stringJoiner.toString());
                    }
                }).setParallelism(1);
        // 如果给WindowOperator设置了sideOutputLateData属性，那么我们可以通过对应的OutputTag来获取迟到的数据
        DataStream<Tuple2<String, Long>> lateStream = stringStream.getSideOutput(lateTag);
        stringStream.print("success").setParallelism(4);
        lateStream.print("late").setParallelism(4);
    }

    public void idleUsage() {
        StreamExecutionEnvironment streamExecutionEnvironment = getStreamExecutionEnvironment();
        DataStreamSource<Tuple2<String, Long>> hasWatermarkStream = streamExecutionEnvironment.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                while (true) {
                    Tuple2<String, Long> tuple2 = Tuple2.of("A", System.currentTimeMillis());
                    ctx.collectWithTimestamp(tuple2, tuple2.f1);
                    ctx.emitWatermark(new Watermark(tuple2.f1));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        DataStreamSource<Tuple2<String, Long>> withoutWatermarkStream = streamExecutionEnvironment.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                int i = 1;
                while (true) {
                    if (i++ == 1) {
                        ctx.collectWithTimestamp(Tuple2.of("B", System.currentTimeMillis()), System.currentTimeMillis() + 9000);
                    }
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        WatermarkStrategy<Tuple2<String, Long>> watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3));
        watermarkStrategy = watermarkStrategy.withTimestampAssigner((element, recordTimestamp) -> element.f1).withIdleness(Duration.ofSeconds(20));

        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStream = withoutWatermarkStream.assignTimestampsAndWatermarks(watermarkStrategy);
        SingleOutputStreamOperator<String> stringStream = hasWatermarkStream.connect(tuple2SingleOutputStream).keyBy(tuple -> tuple.f0, tuple -> tuple.f0).process(new KeyedCoProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>, String>() {
            @Override
            public void processElement1(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value.f0);
            }

            @Override
            public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value.f0);
            }
        }).setParallelism(2);

        DataStreamSink<String> printSink = stringStream.print().setParallelism(3);
    }

    public void test1(){
        StreamExecutionEnvironment streamExecutionEnvironment = getStreamExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = streamExecutionEnvironment.fromElements(Tuple2.of("A", 1000), Tuple2.of("B", 8000));
        WatermarkStrategy<Tuple2<String, Integer>> watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();
        watermarkStrategy.withTimestampAssigner(TimestampAssignerSupplier.of((element, recordTimestamp) -> element.f1));
    }
}
