package com.mzq.hello.flink.sql.udf.aggregation;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.ArrayList;
import java.util.List;

public class AliasSearchAggregate extends AggregateFunction<String, List<String>> {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> redisConnection;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        String redisUrl = context.getJobParameter("redis.url", null);
        redisClient = RedisClient.create(redisUrl);
        redisConnection = redisClient.connect();
    }

    @Override
    public void close() throws Exception {
        super.close();

        redisConnection.close();
        redisClient.shutdown();
    }

    @Override
    public String getValue(List<String> accumulator) {
        return String.join(",", accumulator);
    }

    @Override
    public List<String> createAccumulator() {
        return new ArrayList<>(10);
    }

    public void accumulate(List<String> accumulator, String element) {
        String alias = redisConnection.sync().get("alias_" + element);
        accumulator.add(String.format("%s_%s", element, alias));
    }

    public void retract(List<String> accumulator, String element) {
        String alias = redisConnection.sync().get("alias_" + element);
        accumulator.remove(String.format("%s_%s", element, alias));
    }
}
