package com.mzq.hello.flink;

import com.mzq.hello.domain.WaybillC;
import com.mzq.hello.flink.func.WaybillCSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class HelloWorldFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        DataStreamSource<WaybillC> waybillCDataStreamSource = streamExecutionEnvironment.addSource(new WaybillCSource());
         /*
            既然flink接收上游算子发来的数据后，是一个一个顺序调AsyncFunction方法，需要我们自己在AsyncFunction方法内异步处理元素的话，那么很自然会想到的一个问题是：
            我为什么要用AsyncDataStream？我在MapFunction的实现里也可以使用异步地方式来处理元素，达到异步处理上游数据的效果呀。
            答案是：AsyncDataStream可以对按照上游发送来的数据的顺序，来向下游输出数据。
            例如：假如在MapFunction中使用异步的方式处理数据，那么假设上游发过来的数据是A、B、C，在经过MapFunction的异步处理后，有可能向下游输出的是B、C、A（因为有可能B和C在异步处理中先处理完，而A处理的较慢，后处理完）
            这就导致了Map算子接收上游发过来的数据顺序和Map算子处理完并发送到下游算子的数据顺序不一致，这在对数据顺序比较敏感的场景下是不允许的
            而AsyncDataStream.orderedWait方法，会保证异步处理数据的顺序，也就是说即使异步情况下，C先处理完、B再处理完、A再处理完的情况，orderedWait方法也会保证算子输出到下游时，是以A、B、C的顺序发送的
         */
        SingleOutputStreamOperator<String> waybillCodeStream = AsyncDataStream.unorderedWait(waybillCDataStreamSource,
                // 注意：flink在执行AsyncFunction时，并不是异步来执行的。是需要asyncInvoke自己来实现异步的处理。
                // 如果asyncInvoke的实现不是采用异步形式的话，那么处理上游的元素时，其实是串行处理
                new AsyncFunction<WaybillC, String>() {
                    @Override
                    public void asyncInvoke(WaybillC input, ResultFuture<String> resultFuture) throws Exception {
                        CompletableFuture.supplyAsync(input::getWaybillCode)
                                .thenAcceptAsync(
                                        waybillCode -> {
//                                            try {
//                                                Thread.sleep(RandomUtils.nextLong(1000, 3000));
//                                            } catch (InterruptedException e) {
//                                                e.printStackTrace();
//                                            }
                                            resultFuture.complete(Collections.singleton(waybillCode));
                                        });
                    }

                    @Override
                    public void timeout(WaybillC input, ResultFuture<String> resultFuture) throws Exception {

                    }
                }, 2, TimeUnit.SECONDS);

        waybillCodeStream.print();
        streamExecutionEnvironment.execute();
    }
}
