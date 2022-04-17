package com.mzq.usage.haddop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MyMapredJob {

    private static final Logger logger= LoggerFactory.getLogger(MyMapredJob.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("test");

        Configuration configuration = new Configuration();
        System.setProperty("HADOOP_USER_NAME","supergroup");

        Job job = Job.getInstance(configuration);
        job.setJobName("my-mapred");
        job.setJarByClass(MyMapredJob.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);
        TextInputFormat.addInputPath(job, new Path("/upload/data-center-web-info.log"));
        Path outputPath = new Path("/upload/test123");
        TextOutputFormat.setOutputPath(job, outputPath);

        FileSystem fileSystem = outputPath.getFileSystem(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        job.waitForCompletion(false);

    }

}
