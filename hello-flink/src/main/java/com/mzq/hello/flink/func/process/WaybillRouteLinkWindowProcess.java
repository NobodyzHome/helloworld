package com.mzq.hello.flink.func.process;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WaybillRouteLinkWindowProcess extends ProcessWindowFunction<String, Tuple2<String, String>, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<String> elements, Collector<Tuple2<String, String>> out) throws Exception {
        String summaryResult = elements.iterator().next();
        Tuple2<String, String> tuple2 = Tuple2.of(key, summaryResult);
        out.collect(tuple2);
    }
}
