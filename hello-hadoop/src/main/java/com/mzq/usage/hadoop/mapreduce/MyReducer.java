package com.mzq.usage.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 总结：
 * 1.Mapper的KEYIN和VALUEIN的类型必须与InputFormat的要求匹配
 * 2.Mapper的KEYOUT和VALUEOUT的类型是自己定义的，赋值的类型必须实现WritableComparable接口
 * 3.Combiner的KEYIN和VALUEIN的类型必须和Mapper的KEYOUT和VALUEOUT相同
 * 4.Combiner的KEYOUT和VALUEOUT的类型必须也和Mapper的KEYOUT和VALUEOUT相同
 * 5.Reducer的KEYIN和VALUEIN的类型与Mapper或Combiner的KEYOUT和VALUEOUT的类型一致
 * 6.Reducer的KEYOUT和VALUEOUT的类型是自己定义的，必须与OutputFormat的要求匹配
 */
public class MyReducer extends Reducer<Text, IntWritable, String, Integer> {

    /**
     * @param key     key是当前分组的key
     * @param values  values是当前分组的所有value
     * @param context context是reduce执行的上下文。它的KEYOUT范型是输出的key的类型，VALUEOUT范型是输出的value的类型。他们的具体赋值类型都必须和使用的OutputFormat的key和value匹配。
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, String, Integer>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : values) {
            sum += intWritable.get();
        }
        context.write(key.toString(), sum);
    }

}
