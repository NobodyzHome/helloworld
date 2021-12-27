package com.mzq.hello.flink.sql.udf.aggregation;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.ArrayList;
import java.util.List;

public class AliasSearchAggregate extends AggregateFunction<String, List<String>> {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> redisConnection;
    private Counter counter;
    private Meter meter;
    private Gauge<Long> gauge;
    private Long usage;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        String redisUrl = context.getJobParameter("redis.url", null);
        redisClient = RedisClient.create(redisUrl);
        redisConnection = redisClient.connect();
        MetricGroup udfGroup = context.getMetricGroup().addGroup("udf-metric");
        counter = udfGroup.counter("metric-count");
        meter = udfGroup.meter("meter-test", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
        gauge = context.getMetricGroup().gauge("gauge-test", () -> usage);
    }

    @Override
    public void close() throws Exception {
        super.close();

        redisConnection.close();
        redisClient.shutdown();
    }

    @Override
    public String getValue(List<String> accumulator) {
        counter.inc();
        meter.markEvent();
        return String.join(",", accumulator);
    }

    @Override
    public List<String> createAccumulator() {
        return new ArrayList<>(10);
    }

    public void accumulate(List<String> accumulator, String element) {
        long start = System.currentTimeMillis();
        try {
            Thread.sleep(RandomUtils.nextInt(100,1000));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String alias = redisConnection.sync().get("alias_" + element);
        long end = System.currentTimeMillis();
        usage = end - start;
        accumulator.add(String.format("%s_%s", element, alias));
    }

    public void retract(List<String> accumulator, String element) {
        String alias = redisConnection.sync().get("alias_" + element);
        accumulator.remove(String.format("%s_%s", element, alias));
    }
}
