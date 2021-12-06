package com.mzq.hello.flink.sql.udf.scalar;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * 使用@FunctionHint给函数的返回值类型提供统一的flinksql类型的映射
 *
 * @author maziqiang
 */
@FunctionHint(output = @DataTypeHint("DECIMAL(5,2)"))
public class RedisQuery extends ScalarFunction {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connect;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        String redisUrl = context.getJobParameter("redis.url", null);
        redisClient = RedisClient.create(redisUrl);
        connect = redisClient.connect();
    }

    @Override
    public void close() throws Exception {
        super.close();
        connect.close();
        redisClient.shutdown();
    }


    /**
     * 正常情况下，我们直接根据java的类型就可以告诉flink函数返回值的类型。但某些情况下，我们要告诉flink函数返回的数据的更确切的类型。例如在这里，我们就需要告诉flink，函数返回的是DECIMAL(5,2)的数据类型
     * 使用@DataTypeHint注解（或@FunctionHint注解）告诉flink，返回值是一个整个数字长度是5，小数位长度是2的数字（也就是说整数位只能有3位数字，小数位固定有两位）
     * 注意：如果函数返回的BigDecimal的整数位超过我们在@DataTypeHint里指定的（在这里是3），也就是方法的执行结果和@DataTypeHint定义的返回值类型有出入。flink不会报错，但是flinksql在执行该函数时，返回的是null。
     * 例如redis_query('test')，在执行eval方法时返回的是1234.56，那么redis_query('test')的返回值是null。
     */
    public BigDecimal eval(String name) {
        if (StringUtils.isNotBlank(name)) {
            String numValue = connect.sync().get("num_" + name);
            if (StringUtils.isNotBlank(numValue)) {
                return new BigDecimal(numValue).setScale(2, BigDecimal.ROUND_UP);
            } else {
                return null;
            }
        }
        return null;
    }

    /**
     * eval方法的数量和类型决定了udf函数能接受的入参个数和类型
     *
     * @param id
     * @return
     */
    public BigDecimal eval(int id, String name) {
        BigDecimal result = eval(name);
        if (Objects.isNull(result)) {
            String numValue = connect.sync().get("num_byid_" + id);
            if (StringUtils.isNotBlank(numValue)) {
                result = new BigDecimal(numValue).setScale(2, BigDecimal.ROUND_UP);
            }
        }
        return result;
    }
}
