package com.mzq.usage.hadoop.mapreduce;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;

@Slf4j
public class SplitUsage {

    public static void main(String[] args) throws IOException {
        JobConf jobConf = new JobConf(true);
        TextInputFormat.addInputPath(jobConf, new Path("/data/employee"));

        TextInputFormat textInputFormat = new TextInputFormat();
        // 创建完TextInputFormat对象后，必须要调用configure方法，才能将JobConf中的配置同步给TextInputFormat（例如输入路径）
        textInputFormat.configure(jobConf);
        // 使用InputFormat来生成切片。在这里会根据输入文件去namenode获取block清单，然后再根据切片配置来生成切片清单
        InputSplit[] splits = textInputFormat.getSplits(jobConf, 5);
        // loadDefaults=true时，会将classpath下的core-site.xml、hdfs-site.xml、yarn-site.xml、mapred-site.xml中的内容加载到Configuration对象中
        // 我们有了该对象，就能知道：
        // 1.hdfs访问路径在哪儿
        // 2.mapreduce任务提交到哪儿
        // 3.yarn集群在哪儿
        Configuration configuration = new Configuration(true);
        for (InputSplit inputSplit : splits) {
            FileSplit fileSplit = (FileSplit) inputSplit;
            Path path = fileSplit.getPath();
            // location的作用，告知该切片的数据位于哪些机器中。ResourceManager在为MapTask分配机器时，会根据切片的locations属性，优先分配locations属性里的机器。如果locations指定的NodeManager都没有资源了，ResourceManager就只能为该切片分配其他有资源的NodeManager。
            String locations = String.join(",", fileSplit.getLocations());
            long start = fileSplit.getStart();
            long length = fileSplit.getLength();

            String contents;
            try (FileSystem fileSystem = path.getFileSystem(configuration);
                 FSDataInputStream fsDataInputStream = fileSystem.open(path)) {
                // 在拿到一个FileSplit需要读取时，其实不需要关心该split的内容在几个block里。只需要使用FSDataInputStream，按照FileSplit指示的start跳转到文件的指定位置，然后读取FileSplit指示的length长度的数据即可。
                // 至于读取的数据跨没跨block，跨了几个block，完全是由hdfs内部来判断，对使用者来说是透明的
                fsDataInputStream.seek(start);
                byte[] buffer = new byte[(int) length];
                // 仅读取FileSplit的length指示的长度的数据
                fsDataInputStream.read(buffer);
                contents = new String(buffer);
            }
            log.info("path={},locations={},start={},length={},contents={}", path, locations, start, length, contents.split("\n")[0]);
        }
    }
}
