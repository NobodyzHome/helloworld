package com.mzq.usage.hadoop.hbase.mapred.hbaseToFile;

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

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
        CellScanner cellScanner = value.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String cellValue = Bytes.toString(CellUtil.cloneValue(cell));

            context.write(key, new Text(String.format("%s,%s,%s,%d", family, qualifier, cellValue, cell.getTimestamp())));
        }
    }
}
