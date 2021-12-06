package com.mzq.hello.flink.sql.udf.table;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

/**
 * 由于我们给出的TableFunction的返回值类型为udf类型，所以我们不需要在类上增加@DataTypeHint注解，flinksql可以自动提取对应的flinksql的数据类型
 *
 * @author maziqiang
 */
public class AliasRedisSearch extends TableFunction<AliasSearchResult> {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connect;

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
            collect(new AliasSearchResult(id, key, value));
        }
    }
}