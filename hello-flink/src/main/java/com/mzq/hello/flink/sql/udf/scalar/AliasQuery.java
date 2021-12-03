package com.mzq.hello.flink.sql.udf.scalar;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 所有udf scalar function需要继承自ScalarFunction
 */
public class AliasQuery extends ScalarFunction {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> stringStatefulRedisConnection;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        redisClient = RedisClient.create(context.getJobParameter("redis.url", null));
        stringStatefulRedisConnection = redisClient.connect();
    }

    @Override
    public void close() throws Exception {
        super.close();

        stringStatefulRedisConnection.close();
        redisClient.shutdown();
    }

    /**
     * Scalar function只需要增加eval方法，可以增加多个重载的eval方法，让flinksql调用该函数时可以传入多种参数
     */
    public String eval(int id) {
        String key = "alias_id_" + id;
        return stringStatefulRedisConnection.sync().get(key);
    }

    public String eval(int id, String name) {
        String alias = eval(id);
        if (StringUtils.isBlank(alias)) {
            String key = "alias_name_" + name;
            alias = stringStatefulRedisConnection.sync().get(key);
        }
        return alias;
    }
}
