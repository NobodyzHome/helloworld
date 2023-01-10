package com.mzq.hello.flink.sql.udf.scalar;

import org.apache.flink.table.functions.ScalarFunction;

public class HashCodeScalar extends ScalarFunction {

    public Integer eval(String str) {
        return str == null ? null : str.hashCode();
    }
}
