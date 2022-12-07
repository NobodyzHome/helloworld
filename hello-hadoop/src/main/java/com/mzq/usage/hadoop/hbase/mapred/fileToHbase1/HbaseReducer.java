package com.mzq.usage.hadoop.hbase.mapred.fileToHbase1;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HbaseReducer extends TableReducer<Text, Text, NullWritable> {

    private String separator;

    @Override
    protected void setup(Reducer<Text, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        separator = context.getConfiguration().get("my.job.text.separator");
    }

    @Override
    protected void cleanup(Reducer<Text, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        separator = null;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        Put put = new Put(key.getBytes());
        for (Text value : values) {
            String[] split = value.toString().split(separator);
            String rowkey = split[0];
            String family = split[1];
            String qualifier = split[2];
            String cellValue = split[3];
            long ts = Long.parseLong(split[4]);

            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(cellValue));
        }
        context.write(NullWritable.get(), put);
    }
}
