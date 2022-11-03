package com.mzq.usage.hadoop.spark;

import com.mzq.usage.Employee;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class EmployeeUsage {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://spark-master:7077").setAppName("employee").setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});

        try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
            JavaPairRDD<LongWritable, Text> hadoopFileRDD = javaSparkContext.hadoopFile("hdfs:///data/employee", TextInputFormat.class, LongWritable.class, Text.class);
            JavaRDD<Employee> mapRDD = hadoopFileRDD.map(tuple -> Employee.from(tuple._2.toString()));
            JavaPairRDD<String, Employee> keyByDeptRDD = mapRDD.keyBy(Employee::getDept_no);
            JavaPairRDD<String, Integer> aggregateRDD = keyByDeptRDD.aggregateByKey(0, (i, e) -> i + 1, Integer::sum);
            JavaRDD<String> resultRDD = aggregateRDD.map(tuple -> String.format("%s:%d", tuple._1, tuple._2));
            List<String> collect = resultRDD.collect();
            System.out.println(collect);
        }
    }
}
