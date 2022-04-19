package com.mzq.usage.haddop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
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
