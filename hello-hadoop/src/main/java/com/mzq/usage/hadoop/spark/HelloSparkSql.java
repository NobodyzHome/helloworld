package com.mzq.usage.hadoop.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;

@Slf4j
public class HelloSparkSql {

    public static void main(String[] args) {
//        test1();
        test2();
    }

    public static void test1() {
        SparkConf conf = new SparkConf();
        conf.setMaster("spark://spark-master:7077").setAppName("staff-statistic").setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});

        try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
            JavaRDD<String> fileRDD = javaSparkContext.textFile("hdfs:///upload/staff", 1);
            JavaRDD<StaffInfo> mapRDD = fileRDD.map(str -> {
                String[] split = str.split(",");
                StaffInfo staffInfo = new StaffInfo();
                staffInfo.setName(split[0]);
                staffInfo.setAge(Integer.parseInt(split[1]));
                staffInfo.setSex(split[2]);
                staffInfo.setEducation(split[3]);
                staffInfo.setPolicy(split[4]);
                return staffInfo;
            });
            JavaRDD<String> jsonRDD = mapRDD.map(staff -> new ObjectMapper().writeValueAsString(staff));
            jsonRDD.saveAsTextFile("hdfs:///upload/output");
        }
    }

    public static void test2() {
        SparkConf conf = new SparkConf();
        conf.setMaster("spark://spark-master:7077").setAppName("staff-statistic").setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});

        try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
             SparkSession sparkSession = new SparkSession(JavaSparkContext.toSparkContext(javaSparkContext))) {
            sparkSession.read().format("json").load("hdfs:///upload/output/part-00000").createTempView("staff_info");
            sparkSession.sql("show tables").show();
            sparkSession.sql("select sex,max(age) from staff_info group by sex").show();
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }
}