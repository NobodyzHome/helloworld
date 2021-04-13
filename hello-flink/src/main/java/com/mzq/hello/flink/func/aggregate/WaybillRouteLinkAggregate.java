package com.mzq.hello.flink.func.aggregate;

import com.mzq.hello.domain.WaybillRouteLink;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

public class WaybillRouteLinkAggregate implements AggregateFunction<WaybillRouteLink, List<String>, String> {

    @Override
    public List<String> createAccumulator() {
        return new ArrayList<>(10);
    }

    @Override
    public List<String> add(WaybillRouteLink value, List<String> accumulator) {
        accumulator.add(value.getPackageCode());
        return accumulator;
    }

    @Override
    public String getResult(List<String> accumulator) {
        return String.join(",", accumulator);
    }

    @Override
    public List<String> merge(List<String> a, List<String> b) {
        a.addAll(b);
        return a;
    }
}
