package com.mzq.usage.hadoop.hbase.mapred.hbaseToFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce任务可以实现Tool接口，然后由ToolRunner进行调用，由ToolRunner帮助做一些基本操作。
 * 例如ToolRunner内部会使用GenericOptionsParser将mapreduce的启动参数都赋值给Configuration，省去了用户自己写这块的代码。
 */
public class HbaseToFileJob implements Tool {

    private Configuration configuration;

    /**
     * ToolRunner在调Tool.run方法时，已经把启动参数中所有和mapreduce相同的参数都去掉了，也就是args参数中只有除mapreduce参数以外的启动参数了。
     * 实际就是将GenericOptionsParser.getRemainingArgs()返回的参数传入给Tool的run方法
     */
    @Override
    public int run(String[] args) throws Exception {
        Path outputDir = new Path("/data/hbase_result");
        try (FileSystem fileSystem = outputDir.getFileSystem(getConf())) {
            if (fileSystem.exists(outputDir)) {
                fileSystem.delete(outputDir, true);
            }
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("hbase-to-file");
        job.setJar("/Users/maziqiang/IdeaProjects/helloworld/hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar");
        TableMapReduceUtil.initTableMapperJob("emp", new Scan(), HbaseMapper.class, ImmutableBytesWritable.class, Text.class, job);
        job.setReducerClass(RowKeyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, outputDir);

        job.waitForCompletion(true);
        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 设置自定义参数
        configuration.set("my.rowkey.seperator", ",");
        Configuration hbaseConfiguration = HBaseConfiguration.create(configuration);
        // 启动mapreduce任务
        ToolRunner.run(hbaseConfiguration, new HbaseToFileJob(), args);
    }
}
