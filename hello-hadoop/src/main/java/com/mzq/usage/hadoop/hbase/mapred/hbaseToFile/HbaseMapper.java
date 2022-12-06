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

public class HbaseMapper extends TableMapper<Text, Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
        // TableMapper中入参的key就是rowkey，value就是通过Scan后，该rowkey对应Result，也就是该行数据的cell
        CellScanner cellScanner = value.cellScanner();
        while (cellScanner.advance()) {
            Cell current = cellScanner.current();
            // 在这里我们将每个cell的"column+qualifier"作为输出key，将"rowkey+value"作为输出value，让Reducer将相同column+qualifier的所有rowkey的数据都统计出来
            context.write(new Text(Bytes.toString(CellUtil.cloneFamily(current)) + ":" + Bytes.toString(CellUtil.cloneQualifier(current))),
                    new Text(new String(key.get()) + "-" + Bytes.toString(CellUtil.cloneValue(current))));
        }
    }
}
