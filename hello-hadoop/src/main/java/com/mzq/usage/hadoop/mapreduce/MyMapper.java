package com.mzq.usage.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * @param key     该值是由InputFormat读取数据并解析出来的key，所以他的类型必须和InputFormat的key相同。例如TextInputFormat返回的key的值是当前拉取的这行数据的第一个字符在整个文件中的offset
     * @param value   该值是由InputFormat读取数据并解析出来的value，所以他的类型必须和InputFormat的value相同。例如TextInputFormat返回的value的值是拉取到的这行数据。
     * @param context map执行环境的上下文。它的KEYIN范型是map输出的key的类型，KEYOUT范型是map输出的value的类型。具体赋值的类型必须是WritableComparable的子类。
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while (stringTokenizer.hasMoreElements()) {
            String element = (String) stringTokenizer.nextElement();
            if (element != null) {
                try {
                    context.write(new Text(element), new IntWritable(1));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
