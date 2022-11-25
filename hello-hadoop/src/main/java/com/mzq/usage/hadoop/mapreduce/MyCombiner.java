package com.mzq.usage.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 1.Combiner的KEYIN和VALUEIN的类型必须和Mapper的KEYOUT和VALUEOUT相同
     * 2.Combiner的KEYOUT和VALUEOUT的类型必须也和Mapper的KEYOUT和VALUEOUT相同
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
