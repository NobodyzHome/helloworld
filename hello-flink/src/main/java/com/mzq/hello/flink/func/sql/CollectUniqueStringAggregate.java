package com.mzq.hello.flink.func.sql;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.springframework.format.annotation.DateTimeFormat;

import java.util.*;
import java.util.stream.Collectors;

@FunctionHint(input = @DataTypeHint("STRING"),output = @DataTypeHint("STRING"))
public class CollectUniqueStringAggregate extends AggregateFunction<String, Map<String,Object>> {

    @Override
    public String getValue(Map<String,Object> accumulator) {
        return accumulator.keySet().stream().distinct().collect(Collectors.joining(","));
    }

    @Override
    public Map<String,Object> createAccumulator() {
        return new HashMap<>(10);
    }

    public void accumulate(Map<String,Object> acc, String element) {
        Optional.ofNullable(element).filter(StringUtils::isNotBlank).ifPresent(p->acc.put(p,null));
    }

    public void retract(Map<String,Object> acc, String element) {
        Optional.ofNullable(element).filter(StringUtils::isNotBlank).ifPresent(acc::remove);
    }
}
