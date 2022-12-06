package com.mzq.usage.hadoop.hbase.mapred.hbaseToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HbaseReducer extends TableReducer<ImmutableBytesWritable, Text, Text> {

    private String separator;

    @Override
    protected void setup(Reducer<ImmutableBytesWritable, Text, Text, Mutation>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration configuration = context.getConfiguration();
        separator = configuration.get("my-job.reducer.separator");
    }

    @Override
    protected void cleanup(Reducer<ImmutableBytesWritable, Text, Text, Mutation>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        separator = null;
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Reducer<ImmutableBytesWritable, Text, Text, Mutation>.Context context) throws IOException, InterruptedException {
        String rowkey = new String(key.get());
        Put put = new Put(Bytes.toBytes(rowkey));
        for (Text text : values) {
            String value = text.toString();
            String[] split = value.split(separator);
            String family = split[0];
            String qualifier = split[1];
            String cellValue = split[2];
            long ts = Long.parseLong(split[3]);

            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(cellValue));
        }
        context.write(new Text(rowkey), put);
    }
}
