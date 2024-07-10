package com.mzq.usage.hadoop.hdfs;

import com.mzq.usage.Employee;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class HdfsUsage {

    public static void main(String[] args) {
//        writeEmployeeJson();
//        writeEmployeeCsv();
        writeEmployeeCsvWithNullPk();

//        File file = new File("/Users/maziqiang/Documents/my-libs/flink-libs/hudi-hadoop-mr-bundle-0.14.0-SNAPSHOT.jar");
//        Configuration configuration = new Configuration(true);
//        Path path = new Path("/user/hive/warehouse/auxlib/hudi-hadoop-mr-bundle-0.14.0-SNAPSHOT.jar");
//
//        try (FileInputStream inputStream = new FileInputStream(file);
//             FileSystem fileSystem = FileSystem.get(configuration);
//             FSDataOutputStream fsDataOutputStream = fileSystem.create(path)) {
//            byte[] buffer = new byte[1024 * 10];
//            while (inputStream.read(buffer) != -1) {
//                fsDataOutputStream.write(buffer);
//            }
//
//
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    public static void writeEmployeeCsv() {
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
                while (i++ <= 3000000) {
                    bufferedWriter.write(Employee.generate().toString());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeEmployeeJson() {
        Configuration configuration = new Configuration(true);
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            Path path = new Path("/data/employee_json");
            boolean exists = fileSystem.exists(path);
            if (exists) {
                fileSystem.delete(path, false);
            }
            try (FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
                 OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
                 BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter, 10485760)) {
                JSONArray jsonArray = new JSONArray(Employee.generate(10000));
                bufferedWriter.write(jsonArray.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeEmployeeCsvWithNullPk() {
        Configuration configuration = new Configuration(true);
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            Path path = new Path("/data/employee_with_null_pk");
            boolean exists = fileSystem.exists(path);
            if (exists) {
                fileSystem.delete(path, false);
            }
            try (FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
                 OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fsDataOutputStream);
                 BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter, 10485760)) {
                Employee[] generate = Employee.generate(10);
                generate[9].setEmp_no(null);
                for(Employee employee:generate){
                    bufferedWriter.write(employee.toString());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
