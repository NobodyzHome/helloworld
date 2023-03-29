package com.mzq.hello.flink.sql.connector.sink;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.Deque;

public class RedisSinkWriter implements SinkWriter<RowData>, ProcessingTimeService.ProcessingTimeCallback {

    private StatefulRedisConnection<String, String> redisConnection;
    private RedisAsyncCommands<String, String> redisAsyncCommands;

    private final String url;
    private RedisClient redisClient;
    private Integer[] metaPositions;

    private final ProcessingTimeService processingTimeService;
    private final Deque<RedisFuture<?>> deque = new ArrayDeque<>(10);
    private final CountBundleTrigger<RowData> countBundleTrigger;

    public RedisSinkWriter(String url, Integer[] metaPositions, ProcessingTimeService processingTimeService) {
        this.url = url;
        this.processingTimeService = processingTimeService;
        this.metaPositions = metaPositions;
        countBundleTrigger = new CountBundleTrigger<>(10);
        countBundleTrigger.registerCallback(() -> this.flush(false));

        redisClient = RedisClient.create(getUrl());
        redisConnection = redisClient.connect();
        redisAsyncCommands = redisConnection.async();
        redisAsyncCommands.setAutoFlushCommands(false);

        processingTimeService.registerTimer(ZonedDateTime.now().plusSeconds(30).toInstant().toEpochMilli(), this);
    }

    public String getUrl() {
        return url;
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        String jimkey = element.getString(0).toString();
        String jimValue = element.getString(1).toString();

        int ttlPosition = RedisDynamicTableSink.RedisMetadata.TTL.ordinal();
        long ttl = element.getLong(metaPositions[ttlPosition]);

        RedisFuture<String> set = redisAsyncCommands.setex(jimkey, ttl, jimValue);
        deque.addLast(set);
        try {
            countBundleTrigger.onElement(element);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (!deque.isEmpty()) {
            redisAsyncCommands.flushCommands();
            while (!deque.isEmpty()) {
                RedisFuture<?> redisResult = deque.poll();
                String error = redisResult.getError();
                if (StringUtils.isNotBlank(error)) {
                    throw new RuntimeException("redis响应异常：" + error);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        redisConnection.close();
        redisConnection = null;
        redisAsyncCommands = null;
        redisClient = null;
    }

    @Override
    public void onProcessingTime(long time) throws IOException, InterruptedException, Exception {
        flush(false);
        countBundleTrigger.reset();

        processingTimeService.registerTimer(ZonedDateTime.now().plusSeconds(30).toInstant().toEpochMilli(), this);
    }
}
