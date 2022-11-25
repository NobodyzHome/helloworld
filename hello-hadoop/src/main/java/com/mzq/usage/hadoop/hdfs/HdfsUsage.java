package com.mzq.usage.hadoop.hdfs;

import com.mzq.usage.Employee;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class HdfsUsage {

    public static void main(String[] args) {
        Configuration configuration = new Configuration(true);
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            Path path = new Path("/data/employee");
            boolean exists = fileSystem.exists(path);
            if (exists) {
                fileSystem.delete(path, false);
            }
            try (FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
                 OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
                 BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter, 10485760)) {
                int i = 1;
                while (i++ <= 10000000) {
                    bufferedWriter.write(Employee.generate().toString());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
