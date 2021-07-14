package com.mzq.hello.flink.func;

import com.mzq.hello.domain.WaybillRouteLink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.StringJoiner;

public class WaybillRouteLinkSummaryProcess extends ProcessWindowFunction<WaybillRouteLink, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<WaybillRouteLink> elements, Collector<String> out) throws Exception {
        StringJoiner stringJoiner = new StringJoiner(",");
        elements.iterator().forEachRemaining(waybillRouteLink -> stringJoiner.add(waybillRouteLink.getPackageCode()));
        out.collect(stringJoiner.toString());
    }
}
