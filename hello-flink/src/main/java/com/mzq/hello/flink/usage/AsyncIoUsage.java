package com.mzq.hello.flink.usage;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mzq.hello.domain.WaybillC;
import com.mzq.hello.flink.func.source.WaybillCSource;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class AsyncIoUsage extends BaseFlinkUsage {

    public AsyncIoUsage(StreamExecutionEnvironment streamExecutionEnvironment) {
        super(streamExecutionEnvironment);
    }

    @Override
    protected String jobName() {
        return AsyncIoUsage.class.getName();
    }

    @Override
    protected void setupStream() throws Exception {
        testUseAsyncIO();
    }

    private void testUseAsyncIO() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = getStreamExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
         /*
            我们知道，每一个StreamTask实例都有一个输入缓冲区和输出缓冲区，StreamTask实例的工作是从输入缓冲区中拿取数据，然后交由StreamOperator来处理，StreamOperator在
            处理完成后，会将数据发送到输出缓冲区中。这个过程（拿取数据 -> 处理数据 -> 发送数据）是串行的，也就是说，假如输入缓冲区中的数据是A、B、C（由老到新），那么要先处理完数据A并把
            它发送到输出缓冲区后，才会处理数据B。也就是说输出缓冲区中的顺序也是A、B、C。
            这里面有一个问题是，如果处理数据的逻辑执行起来很慢（这种情况常出现于处理数据的代码中需要调用外部设备（例如redis、es等），而且使用的是同步方法，需要等服务器响应后才能继续处理），
            那么开始处理A和开始处理B之间就会有很长一段时间的间距，但现实情况是数据B其实并不对数据A的处理结果有依赖，处理数据A时也可以处理数据B，也就是常说的数据A和数据B并发处理。

            异步I/O主要解决的就是这个问题，他可以让【处理数据 -> 发送数据】这个步骤异步来执行，这样拿取完数据A后，就可以异步执行后续操作，flink就可以继续拿取数据B了，
            大大减少了数据A和数据B处理的间隔（这中间的间隔就是同步处理时，数据A的处理时长）。
            因此，如果使用异步I/O算子的话，如果输入缓冲区中的数据是A、B、C（由老到新），那么输出缓冲区中的数据有可能是B、C、A（B处理的最快，C其次，最先到来的A反而处理的最慢（有可能是调数据库时网络不好））。
            因此我们在使用异步I/O算子时，要考虑输入和输出的顺序不一致是否是可以接受的。
            不过异步I/O中也有orderedWait方法，能够既保证数据A、B、C的处理是并行处理的，又能够保证数据输出时的顺序和数据输入时的顺序是一致的。

            既然flink接收上游算子发来的数据后，是一个一个顺序调AsyncFunction方法，需要我们自己在AsyncFunction方法内异步处理元素的话，那么很自然会想到的一个问题是：
            我为什么要用AsyncDataStream？我在MapFunction的实现里也可以使用异步地方式来处理元素，达到异步处理上游数据的效果呀。
            答案是：AsyncDataStream可以对按照上游发送来的数据的顺序，来向下游输出数据。
            例如：假如在MapFunction中使用异步的方式处理数据，那么假设上游发过来的数据是A、B、C，在经过MapFunction的异步处理后，有可能向下游输出的是B、C、A（因为有可能B和C在异步处理中先处理完，而A处理的较慢，后处理完）
            这就导致了Map算子接收上游发过来的数据顺序和Map算子处理完并发送到下游算子的数据顺序不一致，这在对数据顺序比较敏感的场景下是不允许的
            而AsyncDataStream.orderedWait方法，会保证异步处理数据的顺序，也就是说即使异步情况下，C先处理完、B再处理完、A再处理完的情况，orderedWait方法也会保证算子输出到下游时，是以A、B、C的顺序发送的。
            A common confusion that we want to explicitly point out here is that the AsyncFunction is not called in a multi-threaded fashion.
            There exists only one instance of the AsyncFunction and it is called sequentially for each record in the respective partition of the stream.
            Unless the asyncInvoke(...) method returns fast and relies on a callback (by the client), it will not result in proper asynchronous I/O.

            当然，orderedWait肯定要比unorderedWait在性能上稍差一些，因为数据的顺序要在checkpoint state存储一段时间。但即使如此，异步I/O也是要比串行的算子快一些，因为数据A、B、C可以几乎同时处理，而在串行的算子中，数据A要处理完
            才能处理数据B，这中间会有很长的间隔。
            In that case, the stream order is preserved. Result records are emitted in the same order as the asynchronous requests are triggered (the order of the operators input records). To achieve that,
            the operator buffers a result record until all its preceding records are emitted (or timed out). This usually introduces some amount of extra latency and some overhead in checkpointing,
            because records or results are maintained in the checkpointed state for a longer time.
         */
        SingleOutputStreamOperator<String> waybillCodeStream = AsyncDataStream.unorderedWait(waybillCDataStreamSource,
                // 注意：flink在执行AsyncFunction时，并不是异步来执行的。是需要asyncInvoke自己来实现异步的处理。
                // 如果asyncInvoke的实现不是采用异步形式的话，那么处理上游的元素时，其实是串行处理
                new AsyncFunction<WaybillC, String>() {
                    @Override
                    public void asyncInvoke(WaybillC input, ResultFuture<String> resultFuture) throws Exception {
                        CompletableFuture.supplyAsync(input::getWaybillCode)
                                .thenAcceptAsync(waybillCode -> {
                                    try {
                                        Thread.sleep(RandomUtils.nextLong(1000, 3000));
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    // 调用ResultFuture的complete方法后，就是把
                                    resultFuture.complete(Collections.singleton(waybillCode));
                                });
                    }

                    // timeout方法控制当处理某一个算子时长过长后，应该如何处理。默认是抛出异常，我们可以通过覆盖该方法，重写该方法的处理方式
                    @Override
                    public void timeout(WaybillC input, ResultFuture<String> resultFuture) throws Exception {

                    }
                }, 2, TimeUnit.SECONDS);

        waybillCodeStream.print();
        streamExecutionEnvironment.execute();
    }

    private void testUseAsyncIO1() {
        // 先把es索引删了，然后后面消费kafka消息，根据waybillCode从redis里获取数据，再写到es
        StreamExecutionEnvironment streamExecutionEnvironment = getStreamExecutionEnvironment();

        KafkaDeserializationSchema<WaybillC> waybillCKafkaDeserializationSchema = new KafkaDeserializationSchema<WaybillC>() {
            private ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

            @Override
            public boolean isEndOfStream(WaybillC nextElement) {
                return false;
            }

            @Override
            public WaybillC deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                return objectMapper.readValue(record.value(), WaybillC.class);
            }

            @Override
            public TypeInformation<WaybillC> getProducedType() {
                return TypeInformation.of(WaybillC.class);
            }
        };

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "myClient");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        FlinkKafkaConsumer<WaybillC> flinkKafkaConsumer = new FlinkKafkaConsumer<>("waybill-c", waybillCKafkaDeserializationSchema, consumerConfig);
        DataStreamSource<WaybillC> waybillcKafkaSource = streamExecutionEnvironment.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<String> waybillCodeStream = waybillcKafkaSource.map(WaybillC::getWaybillCode).returns(String.class);
        AsyncDataStream.unorderedWait(waybillCodeStream, new RichAsyncFunction<String, WaybillC>() {
            private ObjectMapper objectMapper;
            private RedisClient redisClient;
            private StatefulRedisConnection<String, byte[]> stringStatefulRedisConnection;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
                RedisClient redisClient = RedisClient.create("redis://my-redis:6379/3");
                redisClient.connect(new RedisCodec<String, byte[]>() {
                    @Override
                    public String decodeKey(ByteBuffer bytes) {
                        return null;
                    }

                    @Override
                    public byte[] decodeValue(ByteBuffer bytes) {
                        return new byte[0];
                    }

                    @Override
                    public ByteBuffer encodeKey(String key) {
                        return null;
                    }

                    @Override
                    public ByteBuffer encodeValue(byte[] value) {
                        return null;
                    }
                });
            }

            @Override
            public void close() throws Exception {
                super.close();
                stringStatefulRedisConnection.close();
                redisClient.shutdown();
            }

            @Override
            public void timeout(String input, ResultFuture<WaybillC> resultFuture) throws Exception {

            }

            @Override
            public void asyncInvoke(String input, ResultFuture<WaybillC> resultFuture) throws Exception {

            }
        }, 2, TimeUnit.SECONDS, 100);
    }
}
