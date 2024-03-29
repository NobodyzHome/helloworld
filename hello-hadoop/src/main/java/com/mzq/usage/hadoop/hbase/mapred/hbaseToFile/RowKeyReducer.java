package com.mzq.usage.hadoop.hbase.mapred.hbaseToFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RowKeyReducer extends Reducer<ImmutableBytesWritable, Text, Text, Text> {

    private String rowKeySeperator;

    /**
     * setup和cleanup用于在任务执行前和执行完毕后进行一些操作，例如获取配置文件等
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Reducer<ImmutableBytesWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration configuration = context.getConfiguration();
        rowKeySeperator = configuration.get("my.rowkey.seperator");
    }

    @Override
    protected void cleanup(Reducer<ImmutableBytesWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        rowKeySeperator = null;
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Reducer<ImmutableBytesWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Text rowkey = new Text(new String(key.get()));
        for (Text value : values) {
            context.write(rowkey, value);
        }
    }
}
