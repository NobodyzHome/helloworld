package com.mzq.usage.hadoop.hbase.mapred.hbaseToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HbaseMapper extends TableMapper<ImmutableBytesWritable, Text> {

    private String syncFamily;
    private String separator;

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration configuration = context.getConfiguration();
        syncFamily = configuration.get("my-job.mapper.sync-family");
        separator = configuration.get("my-job.reducer.separator");
    }

    @Override
    protected void cleanup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        syncFamily = null;
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        CellScanner cellScanner = value.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String cellValue = Bytes.toString(CellUtil.cloneValue(cell));
            long ts = cell.getTimestamp();
            if (family.equals(syncFamily)) {
                context.write(key, new Text(String.join(separator, family, qualifier, cellValue, String.valueOf(ts))));
            }
        }
    }
}
