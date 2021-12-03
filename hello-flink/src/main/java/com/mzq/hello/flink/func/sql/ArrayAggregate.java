package com.mzq.hello.flink.func.sql;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// 如果使用inputGroup=InputGroup.ANY，那么输入的元素的类型必须是Object
// 自定义的function，必须使用FunctionHint注解，好让flinksql知道如何对接收的数据进行类型转换
// 如果定义数组类型，那么需要给出数组里面元素的类型，例如可以是ARRAY<STRING>，但不能是ARRAY
@FunctionHint(input = @DataTypeHint(inputGroup = InputGroup.ANY), output = @DataTypeHint(value = "ARRAY<STRING>"))
public class ArrayAggregate extends AggregateFunction<String[], List<String>> {

    @Override
    public String[] getValue(List<String> accumulator) {
        return accumulator.stream().map(Object::toString).toArray(String[]::new);
    }

    @Override
    public List<String> createAccumulator() {
        return new ArrayList<>();
    }

    /**
     * accumulate方法是通过反射调用的，一共接收哪几种数据类型，就可以写几个该方法。flinksql会通过反射，根据接收到的元素的数据类型，调用对应的accumulate方法
     * 由于配置了@FunctionHint(input = @DataTypeHint(inputGroup = InputGroup.ANY))，因此只能有一个入参类型为Object的accumulate方法
     *
     * @param accumulator
     * @param obj
     */
    public void accumulate(List<String> accumulator, Object obj) {
        if (Objects.nonNull(obj) && !accumulator.contains(obj.toString())) {
            accumulator.add(obj.toString());
        }
    }

    public void retract(List<String> acc, Object element) {
        accumulate(acc, element);
    }
}
