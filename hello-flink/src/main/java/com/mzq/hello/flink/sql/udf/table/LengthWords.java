package com.mzq.hello.flink.sql.udf.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Objects;
import java.util.stream.Stream;

@FunctionHint(output = @DataTypeHint("ROW<word string,length int>"))
public class LengthWords extends TableFunction<Row> {

    public void eval(long num) {
        if (num >= 5) {
            String str = "num:" + num;
            collect(Row.of(str, str.length()));
        }
    }

    public void eval(String str) {
        if (Objects.nonNull(str)) {
            collect(Row.of(str, str.length()));
        }
    }

    public void eval(Double... nums) {
        Double sum = Stream.of(nums).reduce(Double::sum).orElse(null);
        String str = "nums sum:" + sum;
        collect(Row.of(str, str.length()));
    }

    /**
     * 可以定义一个方法，以Object形式接收各种类型的参数。但需要在参数上增加@DataTypeHint注解，告诉flink该方法都能接收哪些类型的参数
     *
     * @param obj
     */
    public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
        String str = "obj:" + obj;
        collect(Row.of(str, str.length()));
    }
}
