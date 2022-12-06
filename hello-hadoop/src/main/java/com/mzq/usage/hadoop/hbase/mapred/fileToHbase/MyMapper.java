package com.mzq.usage.hadoop.hbase.mapred.fileToHbase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MapReduce Api有两个版本，一个是新版的，另一个是老版的，建议使用新版api：
 * 1.在org.apache.hadoop.mapreduce包下的Mapper与Reducer是mapreduce新版的api
 * 2.在org.apache.hadoop.mapred包下的Mapper与Reducer是mapreduce老版的api
 */
public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {


    @Override
    protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
