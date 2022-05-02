package com.mzq.usage.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

public class HelloSpark {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://spark-master:7077");
        sparkConf.setAppName("hello_world");
        SparkContext sparkContext = new SparkContext(sparkConf);
        RDD<String> stringRDD1 = sparkContext.textFile("/Users/maziqiang/IdeaProjects/helloworld/.gitignore", 3);
        Object collect = stringRDD1.collect();
        System.out.println(collect);
    }
}
