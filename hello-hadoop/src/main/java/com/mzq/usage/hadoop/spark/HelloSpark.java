package com.mzq.usage.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class HelloSpark {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://spark-master:7077");
        sparkConf.setAppName("hello_world");
        JavaSparkContext sp = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rDD = sp.parallelize(Arrays.asList(1, 2, 3, 4));
        Object collect = rDD.collect();
    }
}
