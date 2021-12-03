package com.mzq.hello.flink.sql.udf.table;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 使用@DataTypeHint在类上加入注解，代表告诉flink这个table function返回的数据类型是什么
 * 所有udf table function必须继承TableFunction类
 */
@DataTypeHint("ROW<id int,name string,param1 string,param2 string>")
public class GenerateRows extends TableFunction<Row> {

    private String param1;
    private String param2;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        param1 = context.getJobParameter("my-param1", null);
        param2 = context.getJobParameter("my-param2", null);
    }

    /**
     * 对于table function，我们需要定义public void eval方法，eval方法中可以有参数，就是在sql中调用table function时传入的参数
     * 例如generate_row()调用的就是eval()方法，generate_row(10,'test-')调用的就是eval(int max, String prefix)方法
     */
    public void eval() {
        int current = 1, max = 10;
        while (current++ <= max) {
            collect(Row.of(RandomUtils.nextInt(1, 1000), RandomStringUtils.random(5, true, true), param1, param2));
        }
    }

    public void eval(int max, String prefix) {
        int current = 1;
        while (current++ <= max) {
            collect(Row.of(RandomUtils.nextInt(1, 1000), prefix + RandomStringUtils.random(5, true, true), param1, param2));
        }
    }

    public void eval(int[] idArray, String[] nameArray) {
        for (int i = 0; i <= idArray.length - 1; i++) {
            int id = idArray[i];
            String name = nameArray[i];
            collect(Row.of(id, name, param1, param2));
        }
    }
}
