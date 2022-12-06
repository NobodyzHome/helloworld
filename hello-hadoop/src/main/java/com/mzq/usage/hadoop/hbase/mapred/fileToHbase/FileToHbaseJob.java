package com.mzq.usage.hadoop.hbase.mapred.fileToHbase;

import com.mzq.usage.Employee;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class FileToHbaseJob implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) {
        Path output = new Path("/data/employee");
        try (FileSystem fileSystem = FileSystem.get(getConf())) {
            if (fileSystem.exists(output)) {
                fileSystem.delete(output, false);
            }

            try (FSDataOutputStream fsDataOutputStream = fileSystem.create(output);
                 OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
                 BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter, 10 * 1024 * 1000)) {
                for (int i = 1; i <= 1000; i++) {
                    bufferedWriter.write(Employee.generate().toString());
                }
                bufferedWriter.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            Job job = Job.getInstance(getConf());
            job.setJobName("file-to-hbase");
            job.setJar("/Users/maziqiang/IdeaProjects/helloworld/hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar");
            // mapper端设置：mapper类、mapper输出的key和value的类型、InputFormat、输入文件
            job.setMapperClass(MyMapper.class);
            FileInputFormat.addInputPath(job, output);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);
            // reducer端配置：reducer类、reducer输出的key和value的类型、OutputFormat、输出文件
            // TableMapReduceUtil.initTableReducerJob里配置了：OutputFormat、输出目标源、Reducer类
            TableMapReduceUtil.initTableReducerJob("emp", HbaseReducer.class, job);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Put.class);

            job.waitForCompletion(true);
        } catch (IOException e) {
            return 1;
        } catch (InterruptedException | ClassNotFoundException e) {
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
        Configuration configuration = new Configuration();
        Configuration hbaseConf = HBaseConfiguration.create(configuration);

        FileToHbaseJob myJob = new FileToHbaseJob();
        ToolRunner.run(hbaseConf, myJob, args);
    }
}
