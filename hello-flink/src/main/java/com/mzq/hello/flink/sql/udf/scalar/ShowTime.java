package com.mzq.hello.flink.sql.udf.scalar;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ShowTime extends ScalarFunction {

    public String eval(LocalDateTime time) {
        return time.format(DateTimeFormatter.ISO_DATE_TIME);
    }

    public String eval(Instant time) {
        return time.toString();
    }
}
