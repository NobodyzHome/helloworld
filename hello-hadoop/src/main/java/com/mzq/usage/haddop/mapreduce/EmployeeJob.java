package com.mzq.usage.haddop.mapreduce;

import com.mzq.usage.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Iterator;

@Slf4j
public class EmployeeJob {

    public static class EmployeeMapper implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Employee employee = Employee.from(value.toString());
            output.collect(new Text(employee.getDept_name()), new Text(employee.getEmp_no()));
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void configure(JobConf job) {

        }
    }

    public static class DeptReducer implements Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int cnt = 0;
            while (values.hasNext()) {
                cnt++;
                values.next();
            }
            output.collect(key, new IntWritable(cnt));
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void configure(JobConf job) {

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        /*
         *  1.mapreduce任务的名字
         *  2.设置mapreduce程序的jar包，客户端需要把jar包提交到hdfs
         *  3.Mapper实现类
         *  4.Reducer实现类
         *  5.任务输入文件路径
         *  6.任务输出文件路径
         *  7.读取输入文件的InputFormat
         *  8.设置MapTask输出的中间数据的key和value的类型（为什么不需要设置输入数据的类型，因为使用的InputFormat决定了输入数据的key和value的类型）
         *  9.写入任务计算结果的OutputFormat
         *  10.设置ReduceTask输出的最终计算结果的key和value的类型
         *  11.任务运行模式(local还是yarn)、切片大小、压缩输出等配置存储于mapred-site.xml文件中
         */
        JobConf jobConf = new JobConf(true);
        // 设置任务名称
        jobConf.setJobName("emp");
        // 设置任务的jar包
        jobConf.setJar("/Users/maziqiang/IdeaProjects/helloworld/hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar");
        // 设置任务的Mapper
        jobConf.setMapperClass(EmployeeMapper.class);
        // 设置任务的Reducer
        jobConf.setReducerClass(DeptReducer.class);
        // 设置任务的输入文件路径
        TextInputFormat.addInputPath(jobConf, new Path("/data/employee"));
        // 设置任务的输出文件路径
        Path outputDir = new Path("/data/result");
        TextOutputFormat.setOutputPath(jobConf, outputDir);
        // 设置读取输入文件的InputFormat
        jobConf.setInputFormat(TextInputFormat.class);
        /*
            为什么要是用Text、IntWritable这些类来替代String、int等类型？
            因为mapreduce是分布式任务，而分布式任务必然涉及到从MapTask到ReduceTask的数据传输。不在同一个机器上的数据要怎么传输？就涉及到了序列化，MapTask将中间数据序列化后写入文本文件中
            ，ReduceTask拉取到数据后反序列化获取到实际的数据。因此，MapTask处理完的数据必须能够进行序列化，而Text、IntWritable这些hadoop的类便是为了更好的支持java对象的序列化。
        */
        // 设置MapTask输出的中间结果数据的key和value的类型
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        // 设置写入任务计算结果的OutputFormat
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);
        // 设置ReduceTask输出的数据的key和value的类型
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        // 如果输出路径已存在，需要先删除该路径，否则提交任务会报错
        try (FileSystem fileSystem = outputDir.getFileSystem(new Configuration(true))) {
            boolean exists = fileSystem.exists(outputDir);
            if (exists) {
                fileSystem.delete(outputDir, true);
            }
        }

        // 提交mapreduce任务
        Job job = Job.getInstance(jobConf);
        job.submit();
    }
}
