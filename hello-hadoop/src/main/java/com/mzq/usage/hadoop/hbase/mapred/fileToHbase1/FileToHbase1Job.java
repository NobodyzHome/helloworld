package com.mzq.usage.hadoop.hbase.mapred.fileToHbase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class FileToHbase1Job implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) {
        String sourceTableName = conf.get("my.job.hbase.source-table");
        String targetTableName = conf.get("my.job.hbase.target-table");

        try (Connection connection = ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {
            TableName targetTable = TableName.valueOf(targetTableName);
            if (admin.tableExists(targetTable)) {
                admin.disableTable(targetTable);
                admin.deleteTable(targetTable);
            }

            TableName sourceTable = TableName.valueOf(sourceTableName);
            HTableDescriptor sourceTableDescriptor = admin.getTableDescriptor(sourceTable);
            HTableDescriptor targetTableDescriptor = new HTableDescriptor(targetTable);
            sourceTableDescriptor.getFamilies().forEach(targetTableDescriptor::addFamily);
            admin.createTable(targetTableDescriptor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            Job job = Job.getInstance(getConf());
            job.setJobName("file-to-hbase1");
            job.setJar("/Users/maziqiang/IdeaProjects/helloworld/hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar");

            job.setMapperClass(MyMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path("/data/hbase_result"));
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            TableMapReduceUtil.initTableReducerJob(targetTableName, HbaseReducer.class, job);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Put.class);

            job.waitForCompletion(false);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
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
        hbaseConf.set("my.job.hbase.source-table", "emp");
        hbaseConf.set("my.job.hbase.target-table", "empBak");
        hbaseConf.set("my.job.text.separator", ",");

        FileToHbase1Job fileToHbase1Job1 = new FileToHbase1Job();
        ToolRunner.run(hbaseConf, fileToHbase1Job1, args);
    }
}
