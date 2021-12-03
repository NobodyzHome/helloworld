package com.mzq.hello.flink.sql.udf.table;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@DataTypeHint("ROW<id int,key string,name string>")
public class AliasRedisSearch extends TableFunction<Row> {

    private RedisClient redisClient;
    StatefulRedisConnection<String, String> connect;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        String uri = context.getJobParameter("redis.url", null);
        redisClient = RedisClient.create(uri);
        connect = redisClient.connect();
    }

    @Override
    public void close() throws Exception {
        super.close();

        connect.close();
        redisClient.shutdown();
    }

    public void eval(int id) {
        RedisCommands<String, String> sync = connect.sync();
        String key = "alias_" + id;
        String value = sync.get(key);
        if (StringUtils.isNotBlank(value)) {
            collect(Row.of(id, key, value));
        }
    }
}