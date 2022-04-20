package com.mzq.usage.haddop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

public class MyMapredJob {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "supergroup");

        Job job = Job.getInstance(configuration);
        job.setJobName("my-mapred");
        // 在提交任务后，mapreduce客户端会从该目录找到程序的jar包，上传到hdfs
        job.setJar("/Users/maziqiang/IdeaProjects/helloworld/hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar");
        // 设置Mapper类
        job.setMapperClass(MyMapper.class);
        // 设置Mapper输出的key的类型
        job.setMapOutputKeyClass(Text.class);
        // 设置Mapper输出的value的类型
        job.setMapOutputValueClass(IntWritable.class);
        // 设置Reducer类
        job.setReducerClass(MyReducer.class);
        // 设置Combiner类
        job.setCombinerClass(MyCombiner.class);
        Path inputPath = new Path("/upload/data-center-web-info.log");
        // 设置读取文件时使用的格式以及输入的文件路径
        TextInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path("/upload/test123");
        // 设置输出文件时使用的格式以及输出的文件路径
        TextOutputFormat.setOutputPath(job, outputPath);

        try (FileSystem fileSystem = outputPath.getFileSystem(configuration)) {
            if (!fileSystem.exists(inputPath)) {
                try (FileInputStream fileInputStream = new FileInputStream(Paths.get("/Users/maziqiang/Downloads/data-center-web-info-2020-07-14-1.log").toFile());
                     FSDataOutputStream fsDataOutputStream = fileSystem.create(inputPath)) {

                    byte[] buffer = new byte[512];

                    while ((fileInputStream.read(buffer)) != -1) {
                        fsDataOutputStream.write(buffer);
                    }
                    fsDataOutputStream.flush();
                }
            }
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
        } catch (Exception e) {
            throw e;
        }

        // 提交任务
        job.waitForCompletion(false);
    }
}
