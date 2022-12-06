package com.mzq.usage.hadoop.hbase.mapred.fileToHbase;

import com.mzq.usage.Employee;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WriteHbaseReducer extends TableReducer<LongWritable, Text, NullWritable> {

    @Override
    protected void setup(Reducer<LongWritable, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        // 由于map端是按照行号作为key的，因此一个key只会有一个value，就是那行数据的内容
        Text value = values.iterator().next();
        String str = value.toString();
        if (StringUtils.isBlank(str)) {
            return;
        }

        Employee employee = Employee.from(str);
        Put put = new Put(Bytes.toBytes(employee.getEmp_no()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(employee.getEmp_name()));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(employee.getSex()));
        put.addColumn(Bytes.toBytes("secret"), Bytes.toBytes("salary"), Bytes.toBytes(employee.getSalary()));
        put.addColumn(Bytes.toBytes("secret"), Bytes.toBytes("create_dt"), Bytes.toBytes(employee.getCreate_dt()));
        put.addColumn(Bytes.toBytes("dept"), Bytes.toBytes("name"), Bytes.toBytes(employee.getDept_name()));
        put.addColumn(Bytes.toBytes("dept"), Bytes.toBytes("no"), Bytes.toBytes(employee.getDept_no()));
        context.write(NullWritable.get(), put);
    }

    @Override
    protected void cleanup(Reducer<LongWritable, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
