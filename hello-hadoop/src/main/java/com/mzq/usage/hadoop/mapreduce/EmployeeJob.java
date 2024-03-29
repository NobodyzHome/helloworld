package com.mzq.usage.hadoop.mapreduce;

import com.mzq.usage.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    public static class DeptReducer implements Reducer<Text, Text, String, Integer> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<String, Integer> output, Reporter reporter) throws IOException {
            int cnt = 0;
            while (values.hasNext()) {
                cnt++;
                values.next();
            }
            output.collect(key.toString(), cnt);
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
         *  一个mapreduce任务最基础需要配置的内容：
         *  1.mapreduce任务的名字
         *  2.设置mapreduce程序的jar包，客户端需要把jar包提交到hdfs
         *  3.Mapper实现类
         *  4.Reducer实现类
         *  5.任务输入文件路径
         *  6.任务输出文件路径
         *  7.设置用于读取输入文件内容并转换为key、value（也就是Mapper.map方法的第一和第二个入参）的InputFormat
         *  8.设置MapTask输出的中间数据的key和value的类型（为什么不需要设置输入数据的类型，因为使用的InputFormat决定了输入数据的key和value的类型）
         *  9.设置用于将Reducer.reduce输出的key、value写入到输出文件的OutputFormat
         *  10.设置ReduceTask输出的最终计算结果的key和value的类型
         *  11.任务运行模式(local还是yarn)、切片大小、压缩输出等配置存储于mapred-site.xml文件中
         *
         *  MapReduce中数据的流转：
         *  输入文件 --> RecordReader（通过InputFormat.getRecordReader()获取）解析成key,value --> Mapper.map --> 序列化中间结果 --> 网络传输 --> 反序列化中间数据 --> Reducer.reduce计算出key,value --> Collector.collect --> RecordWriter(通过OutputFormat.getRecordWriter()获取)写入key,value --> 输出文件
         *  可以看到：
         *  RecordReader是将数据文件读取为key、value；RecordWriter是将key、value写入到输出文件。
         *  选用了何种InputFormat决定了如何将数据文件读取成key,value；而选用了何种OutputFormat决定了如何将key,value写入到输出文件。
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
        Path outputDir = new Path("/data/employee_mapreduce_result");
        TextOutputFormat.setOutputPath(jobConf, outputDir);
        // 设置读取输入文件的InputFormat
        // TextInputFormat每次调用next方法是读取当前数据文件的一行，然后把当前行第一个字符的offset作为key，把当前行的内容作为value。key和value会传入Mapper.map方法
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
        // TextOutputFormat是把Reducer.reduce中collect的key和value按照key\tvalue的格式写入到目的地
        jobConf.setOutputFormat(TextOutputFormat.class);
        // 设置ReduceTask输出的数据的key和value的类型，TextOutputFormat不要求输出的key和value必须是hadoop的writable的
        jobConf.setOutputKeyClass(String.class);
        jobConf.setOutputValueClass(Integer.class);

        // 如果输出路径已存在，需要先删除该路径，否则提交任务会报错
        // path.getFileSystem()会根据classpath下core-site.xml的fs.defaultFS配置来知道要使用哪个文件系统以及文件系统的连接地址
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
