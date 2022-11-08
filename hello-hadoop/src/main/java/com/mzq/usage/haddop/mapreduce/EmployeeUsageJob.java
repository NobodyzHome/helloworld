package com.mzq.usage.haddop.mapreduce;

import com.mzq.usage.Employee;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringJoiner;

public class EmployeeUsageJob {

    public static class EmpMapper implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            Employee employee = Employee.from(value.toString());
            output.collect(new Text(employee.getDept_name()), new Text(employee.getEmp_name()));
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void configure(JobConf job) {

        }
    }

    public static class EmpReducer implements Reducer<Text, Text, String, String> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<String, String> output, Reporter reporter) throws IOException {
            StringJoiner sj = new StringJoiner(",");
            while (values.hasNext()) {
                sj.add(values.next().toString());
            }
            output.collect(key.toString(), sj.toString());
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void configure(JobConf job) {

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf jobConf = new JobConf(true);
        jobConf.setJobName("employee-usage");
        jobConf.setJar("/Users/maziqiang/IdeaProjects/helloworld/hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar");
        jobConf.setMapperClass(EmpMapper.class);
        jobConf.setReducerClass(EmpReducer.class);
        // 设置输入
        TextInputFormat.addInputPath(jobConf, new Path("/data/employee"));
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        // 设置输出
        Path output = new Path("/data/employee_mapred_usage_result");
        TextOutputFormat.setOutputPath(jobConf, output);
        jobConf.setOutputFormat(TextOutputFormat.class);
        jobConf.setOutputKeyClass(String.class);
        jobConf.setOutputValueClass(String.class);

        try (FileSystem fileSystem = output.getFileSystem(jobConf)) {
            if (fileSystem.exists(output)) {
                fileSystem.delete(output, true);
            }
        }

        Job job = Job.getInstance(jobConf);
        job.submit();
    }
}
