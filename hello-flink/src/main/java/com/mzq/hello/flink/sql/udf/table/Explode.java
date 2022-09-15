package com.mzq.hello.flink.sql.udf.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.TableFunction;

import java.util.stream.Stream;

public class Explode extends TableFunction<String> {

    public void eval(String str) {
        if (StringUtils.isNotBlank(str)) {
            Stream.of(str.split(",")).forEach(this::collect);
        }
    }
}
