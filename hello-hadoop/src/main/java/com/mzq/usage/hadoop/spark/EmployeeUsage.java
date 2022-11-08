package com.mzq.usage.hadoop.spark;

import com.mzq.usage.Employee;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class EmployeeUsage {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local").setAppName("employee");

        try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaPairRDD<LongWritable, Text> fileRDD = javaSparkContext.hadoopFile("/data/employee", TextInputFormat.class, LongWritable.class, Text.class, 3);
            JavaPairRDD<String, Employee> deptPariRDD = fileRDD.mapToPair(tuple -> {
                Text text = tuple._2;
                Employee employee = Employee.from(text.toString());
                return Tuple2.apply(employee.getDept_name(), employee);
            });
            JavaPairRDD<String, AtomicInteger> aggregateRDD = deptPariRDD.aggregateByKey(new AtomicInteger(0), 5, (a, e) -> {
                a.incrementAndGet();
                return a;
            }, (a, b) -> {
                a.set(a.get() + b.get());
                return a;
            });
            JavaPairRDD<String, Integer> mapRDD = aggregateRDD.mapToPair(tuple -> Tuple2.apply(tuple._1, tuple._2.get()));

            Path path = new Path("/data/employee_spark_result");
            try (FileSystem fileSystem = path.getFileSystem(new Configuration())) {
                if (fileSystem.exists(path)) {
                    fileSystem.delete(path, true);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            mapRDD.saveAsHadoopFile(path.toString(), String.class, Integer.class, TextOutputFormat.class);
        }
    }
}
