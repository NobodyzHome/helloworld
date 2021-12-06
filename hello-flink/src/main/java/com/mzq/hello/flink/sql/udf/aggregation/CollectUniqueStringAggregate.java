package com.mzq.hello.flink.sql.udf.aggregation;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * 1.AggregateFunction中第一个范型参数T是聚合函数返回结果的类型，第二个范型参数ACC是聚合函数在聚合数据过程使用的累加器类型
 * 2.无论是T还是ACC，提供的实现都需要是能提取成flinksql字段类型的类型。
 * 3.由于我们在实现AggregateFunction时，对函数返回结果T的范型已经赋值了，因此不需要使用@DataTypeHint注解在类上，显式给出函数返回的flinksql的数据类型（当然如果要控制返回类型的细节的话，也可以给出）。
 * 4.在类中，使用accumulate和retract方法给出聚合函数能接收的参数
 * <p>
 * flinksql在使用udf函数时，一个很重要的工作就是将java类中的类型提取成flinksql的类型，flinksql在提取数据类型时可以：
 * 1) 根据一部分java类型自动提取成对应的flinksql类型，参照文档:https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/#data-type-extraction
 * 2) 根据用户自定义的类型提取自定义类中都有哪些flinksql类型的字段，参照文档：https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/#user-defined-data-types
 * 3) 使用@DataTypeHint强制指定java中的类型对应flinksql中的类型
 * 4) 使用@FunctionHint指定函数的输入和输出类型对应flinksql中的类型
 *
 * @author maziqiang
 */
public class CollectUniqueStringAggregate extends AggregateFunction<AggregateResult, Map<String, Integer>> {

    @Override
    public AggregateResult getValue(Map<String, Integer> accumulator) {
        String[] strArray = accumulator.keySet().toArray(new String[0]);
        String joined = String.join(",", accumulator.keySet());
        return new AggregateResult(joined, strArray);
    }

    @Override
    public Map<String, Integer> createAccumulator() {
        return new HashMap<>(10);
    }

    /**
     * aggregation function每能接收一种数据类型，则需要加一个accumulate(acc,element)和retract(acc,element)方法
     * 在收到UPDATE_AFTER、INSERT类型的RowData时调用accumulate方法
     */
    public void accumulate(Map<String, Integer> acc, String name, String alias) {
        acc.put(name + "-" + alias, null);
    }

    /**
     * 在收到UPDATE_BEFORE、DELETE类型的RowData时调用retract方法
     */
    public void retract(Map<String, Integer> acc, String name, String alias) {
        acc.remove(name + "-" + alias, null);
    }

    public void accumulate(Map<String, Integer> acc, String name) {
        acc.put(name, null);
    }

    public void retract(Map<String, Integer> acc, String name) {
        acc.remove(name, null);
    }
}
