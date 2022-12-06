package com.mzq.usage.hadoop.hbase.mapred.hbaseToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HbaseToHbaseJob implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) {
        Configuration configuration = getConf();
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             Admin admin = connection.getAdmin()) {

            TableName sourceTable = TableName.valueOf(configuration.get("my-job.hbase.source-table"));
            TableName targetTable = TableName.valueOf(configuration.get("my-job.hbase.target-table"));
            if (!admin.tableExists(targetTable)) {
                HTableDescriptor sourceTableDescriptor = admin.getTableDescriptor(sourceTable);
                HTableDescriptor targetTableDescriptor = new HTableDescriptor(targetTable);

                String syncFamily = configuration.get("my-job.mapper.sync-family");
                HColumnDescriptor columnDescriptor = sourceTableDescriptor.getFamily(Bytes.toBytes(syncFamily));
                columnDescriptor.setMaxVersions(3);
                targetTableDescriptor.addFamily(columnDescriptor);
                admin.createTable(targetTableDescriptor);
            }

            Job job = Job.getInstance(configuration);
            job.setJobName("hbase-to-hbase");
            job.setJar("/Users/maziqiang/IdeaProjects/helloworld/hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar");

            // mapper端：输入文件、InputFormat、map output key、value类型、mapper类
            TableMapReduceUtil.initTableMapperJob("emp", new Scan(), HbaseMapper.class, ImmutableBytesWritable.class, Text.class, job);
            // reducer端：输出文件、OutputFormat、reducer output key、value类型、reducer类
            TableMapReduceUtil.initTableReducerJob("empCopy", HbaseReducer.class, job);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.waitForCompletion(false);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration(true);
        Configuration hbaseConf = HBaseConfiguration.create(configuration);
        hbaseConf.set("my-job.hbase.source-table", "emp");
        hbaseConf.set("my-job.hbase.target-table", "empCopy");
        hbaseConf.set("my-job.mapper.sync-family", "info");
        hbaseConf.set("my-job.reducer.separator", ",");

        HbaseToHbaseJob myJob = new HbaseToHbaseJob();
        ToolRunner.run(hbaseConf, myJob, args);
    }
}
