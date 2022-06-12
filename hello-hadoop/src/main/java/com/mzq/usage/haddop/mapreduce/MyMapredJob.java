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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

public class MyMapredJob {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "supergroup");
        Configuration configuration = new Configuration();
         /*
            通常情况下hadoop的命令都是由bin/hadoop command [genericOptions] [commandOptions]这几部分组成。
            其中genericOptions可以是hadoop的core-site、hdfs-site、mapred-site、yarn-site里配置，这代表着：我们可以在配置文件中给出默认配置，然后我们还可以通过在运行时给出genericOptions，来覆盖默认的配置。
            GenericOptionsParser可以解析main方法中的启动参数，把启动参数中排在最前面的所有的【"-D" "key=value"】这样的启动参数解析成hadoop的配置，其中"-D"代表后面跟的下一个启动参数是hadoop的配置，"key=value"则对应对应的haddop参数的配置内容。

            举例说明，启动参数为：-D mapreduce.framework.name=yarn -D mapreduce.job.reduces=2 output /upload/mapred-result input /upload/data-center-web-info.log appName mapreduce-helloworld
            那么GenericOptionsParser会认为mapreduce.framework.name和mapreduce.job.reduces是hadoop的配置，会用对应的值覆盖默认配置。

            但前提必须是他们都排在启动参数的最前面，如果启动参数为：-D mapreduce.framework.name=yarn output /upload/mapred-result input /upload/data-center-web-info.log appName mapreduce-helloworld -D mapreduce.job.reduces=2
            那么GenericOptionsParser只认为mapreduce.framework.name是hadoop的配置
        */
        GenericOptionsParser genericOptionsParser = new GenericOptionsParser(configuration, args);
        /*
        * 当GenericOptionsParser解析完启动参数后，我们可以使用getRemainingArgs方法，获取启动参数中不是用来定义hadoop参数的启动参数。
        * 例如启动参数为：-D mapreduce.framework.name=yarn -D mapreduce.job.reduces=2
        * 被解析后，getRemainingArgs返回的剩余的启动参数就是：output /upload/mapred-result input /upload/data-center-web-info.log appName mapreduce-helloworld
        */
        String[] remainingArgs = genericOptionsParser.getRemainingArgs();

        if (remainingArgs.length % 2 != 0) {
            throw new IllegalArgumentException("参数需要成对出现！");
        }

        int i = 0;
        Properties customParams = new Properties();
        while (i < remainingArgs.length) {
            customParams.setProperty(remainingArgs[i++], remainingArgs[i++]);
        }

        Job job = Job.getInstance(configuration);
        job.setJobName(customParams.getProperty("appName"));
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
        Path inputPath = new Path(customParams.getProperty("input"));
        // 设置读取文件时使用的格式以及输入的文件路径
        TextInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path(customParams.getProperty("output"));
        // 设置输出文件时使用的格式以及输出的文件路径
        TextOutputFormat.setOutputPath(job, outputPath);

        try (FileSystem fileSystem = outputPath.getFileSystem(configuration)) {
            if (!fileSystem.exists(inputPath)) {
                try (FileInputStream fileInputStream = new FileInputStream(Paths.get("/Users/maziqiang/Documents/data-center-web-info-part.log").toFile());
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
