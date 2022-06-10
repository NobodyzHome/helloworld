package com.mzq.usage.hadoop.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class HelloSparkExcercise {

    public static void main(String[] args) {
//        test1();
//        test2();
//        test3();
//        test4();
//        test5();
        test6();
    }

    public static void test1() {
        Configuration configuration = new Configuration(true);
        Path path = new Path("/upload/data-center-web-info-part.log");
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, false);
            }

            try (FileInputStream fileInputStream = new FileInputStream("/Users/maziqiang/Documents/my-libs/data-center-web-info-part.log");
                 BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                 FSDataOutputStream fsDataOutputStream = fileSystem.create(path)) {

                byte[] buffer = new byte[2048];
                while (bufferedInputStream.read(buffer) != -1) {
                    fsDataOutputStream.write(buffer);
                }
            } catch (Exception e) {
                throw e;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 从配置文件中获取参数，然后赋值到系统参数中，为SparkConf做准备
        Properties properties = new Properties();
        URL resource = HelloSparkExcercise.class.getClassLoader().getResource("spark-site.properties");
        Objects.requireNonNull(resource);
        try (FileInputStream in = new FileInputStream(resource.getFile())) {
            properties.load(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        properties.forEach((key, val) -> System.setProperty((String) key, (String) val));

        // 当创建SparkConf对象时，如果loadDefaults设置为true时，会将java系统参数中（使用-D配置）所有以"spark."开头的参数，都认为是spark的参数，设置到SparkConf对象中。
        SparkConf sparkConf = new SparkConf(true);
        // 设置master，可以是local、spark集群、yarn-client、yarn-cluster(设置为yarn的这两个的话，需要增加org.apache.spark.spark-yarn_2.11的依赖)
        sparkConf.setMaster("spark://spark-master:7077");
        // 设置任务的名称
        sparkConf.setAppName("hello_world");
        sparkConf.setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});
        JavaSparkContext sp = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sp.textFile("hdfs:///upload/data-center-web-info-part.log", 6);
        JavaRDD<String> wordsRDD = stringJavaRDD.flatMap(words -> {
            StringTokenizer stringTokenizer = new StringTokenizer(words, " ");
            List<String> list = new ArrayList<>();
            while (stringTokenizer.hasMoreElements()) {
                String element = (String) stringTokenizer.nextElement();
                list.add(element);
            }
            return list.iterator();
        });
        JavaPairRDD<String, Integer> mapValuesRDD = wordsRDD.groupBy(word -> word).mapValues(words -> {
            int count = 0;
            for (String word : words) {
                count += 1;
            }
            return count;
        });
        Map<String, Integer> stringIntegerMap = mapValuesRDD.filter(tuple -> tuple._2() >= 10).collectAsMap();
        log.info("执行结果：{}", stringIntegerMap);
        sp.close();
    }

    public static void test2() {
        Configuration configuration = new Configuration(true);
        Path path = new Path("/upload/data-center-web-info-part.log");
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, false);
            }

            try (FileInputStream fileInputStream = new FileInputStream("/Users/maziqiang/Documents/my-libs/data-center-web-info-part.log");
                 BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                 FSDataOutputStream fsDataOutputStream = fileSystem.create(path)) {

                byte[] buffer = new byte[2048];
                while (bufferedInputStream.read(buffer) != -1) {
                    fsDataOutputStream.write(buffer);
                }
            } catch (Exception e) {
                throw e;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SparkConf sparkConf = new SparkConf(true);
        sparkConf.setAppName("hello-spark");
        sparkConf.setMaster("spark://spark-master:7077");
        sparkConf.setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> fileRdd = sparkContext.textFile("hdfs:///upload/data-center-web-info-part.log", 5);
            JavaPairRDD<Character, Integer> charRDD = fileRdd.flatMapToPair(s -> {
                List<Tuple2<Character, Integer>> tuple2List = new ArrayList<>(s.length());
                for (Character c : s.toCharArray()) {
                    tuple2List.add(new Tuple2<>(c, 1));
                }
                return tuple2List.iterator();
            });

            List<Tuple2<Character, Integer>> top = charRDD.reduceByKey(Integer::sum).filter(tuple -> tuple._2() > 10).top(10, new TupleComparator());
            System.out.println(top);
        }
    }

    public static void test3() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("hello-world");
        sparkConf.setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});
        sparkConf.setMaster("spark://spark-master:7077");

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> fileRDD = sparkContext.textFile("hdfs:///upload/data-center-web-info.log", 5);

            JavaRDD<String> flatMapRDD = fileRDD.flatMap(s -> {
                Pattern p = Pattern.compile("[A-Za-z]+");
                Matcher matcher = p.matcher(s);

                List<String> list = new ArrayList<>();
                while (matcher.find()) {
                    String substring = s.substring(matcher.start(), matcher.end());
                    list.add(substring.toLowerCase());
                }
                return list.iterator();
            });
            JavaRDD<String> filterRDD = flatMapRDD.filter(s -> StringUtils.startsWithIgnoreCase(s, "ba"));
            JavaPairRDD<String, Integer> pairRDD = filterRDD.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairRDD<String, Integer> coalesceRDD = pairRDD.repartition(2);
            JavaPairRDD<String, Integer> sumRDD = coalesceRDD.reduceByKey(Integer::sum);
            List<Tuple2<String, Integer>> top = sumRDD.top(5, new TupleComparator1());
            System.out.println(top);
        }
    }

    public static void test4() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://spark-master:7077").setAppName("hello-world").setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> fileRDD = sparkContext.textFile("hdfs:///upload/data-center-web-info.log", 5);
            JavaRDD<String> flatMapRDD = fileRDD.flatMap(s -> {
                Pattern p = Pattern.compile("[A-Za-z]+");
                String lowStr = s.toLowerCase();
                Matcher matcher = p.matcher(lowStr);

                List<String> list = new ArrayList<>();
                while (matcher.find()) {
                    list.add(lowStr.substring(matcher.start(), matcher.end()));
                }
                return list.iterator();
            });
            // 统计相同词频的单词
            JavaRDD<String> repartitionRDD = flatMapRDD.repartition(11);
            JavaPairRDD<String, Integer> mapToPairRDD = repartitionRDD.mapToPair(s -> new Tuple2<>(s, 1));
            // 1.先统计每个单词的词频
            JavaPairRDD<String, Integer> reduceRDD = mapToPairRDD.reduceByKey(Integer::sum, 3);
            // 2.将数据倒置，变为<词频,单词>的形式
            JavaPairRDD<Integer, String> reverseRDD = reduceRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
            // 3.将相同词频的数据统计在一起
            JavaPairRDD<Integer, Iterable<String>> groupRDD = reverseRDD.groupByKey(6);
            JavaPairRDD<Integer, String> mapValuesRDD = groupRDD.mapValues(it -> String.join(",", it));
            JavaPairRDD<Integer, String> filterRDD = mapValuesRDD.filter(tuple -> tuple._2.split(",").length >= 5);
            JavaPairRDD<Integer, String> filter1RDD = filterRDD.filter(tuple -> tuple._1 >= 100);
            System.out.println(filter1RDD.toDebugString());
            Map<Integer, String> integerStringMap = filter1RDD.collectAsMap();
            System.out.println(integerStringMap);
        }
    }

    public static void test5() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("spark://spark-master:7077").setAppName("hello-world").setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> fileRDD = sparkContext.textFile("hdfs:///upload/data-center-web-info.log", 10);
            JavaRDD<String> flatMapRDD = fileRDD.flatMap(str -> {
                String[] split = str.split("---");
                if (split.length < 2) {
                    return Collections.emptyIterator();
                } else {
                    return Collections.singleton(StringUtils.trim(split[1])).iterator();
                }
            });
            JavaPairRDD<String, Integer> mapToPairRDD = flatMapRDD.mapToPair(str -> new Tuple2<>(str, 1));
            JavaPairRDD<String, Integer> reduceRDD = mapToPairRDD.reduceByKey(Integer::sum, 6);
            List<Tuple2<String, Integer>> top = reduceRDD.top(3, new TupleComparator1());
            top.forEach(System.out::println);
            System.out.println(reduceRDD.toDebugString());
        }
    }

    public static void test6() {
        Configuration configuration = new Configuration();
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            Path path = new Path("/upload/testData.txt");
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, false);
            }
            try (FileInputStream fileInputStream = new FileInputStream("/Users/maziqiang/Downloads/testData.txt");
                 InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                 BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                 FSDataOutputStream fsDataOutputStream = fileSystem.create(path)) {

                String line;
                while (Objects.nonNull(line = bufferedReader.readLine())) {
                    fsDataOutputStream.write((line + "\n").getBytes());
                }
                fsDataOutputStream.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SparkConf sparkConf = new SparkConf().setMaster("spark://spark-master:7077").setAppName("hello-world").setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});
//        sparkConf.set("spark.executor.memory", "1g").set("spark.executor.instances", "1").set("spark.executor.cores","2");
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> fileRDD = sparkContext.textFile("hdfs:///upload/testData.txt", 1);
            JavaPairRDD<Integer, String> mapToPairRDD = fileRDD.flatMapToPair(s -> {
                if (StringUtils.isBlank(s)) {
                    return Collections.emptyIterator();
                }

                s = StringUtils.trim(s);
                String[] split = s.split(",");
                BigDecimal bigDecimal = new BigDecimal(split[1]).setScale(0, RoundingMode.HALF_UP);
                return Collections.singleton(new Tuple2<>(bigDecimal.intValue(), split[0])).iterator();
            });

            // 先提交一个spark任务，计算公司总人数
            long count = mapToPairRDD.count();
            // 等总人数任务执行完毕后，再提交一个spark任务，计算公司每个司龄的人数以及人数占总公司的占比
            JavaPairRDD<Integer, Iterable<String>> groupRDD = mapToPairRDD.groupByKey();
            JavaPairRDD<Integer, String> mapValuesRDD = groupRDD.mapValues(it -> String.join(",", it));
            JavaRDD<Tuple2<Integer, String>> mapRDD = mapValuesRDD.map(tuple -> {
                int percentage = new BigDecimal(tuple._2.split(",").length)
                        .divide(new BigDecimal(count), 2, RoundingMode.HALF_UP).multiply(new BigDecimal(100)).intValue();
                return new Tuple2<>(percentage, String.format("%d --- %s", tuple._1, tuple._2));
            });
            List<Tuple2<Integer, String>> collect = mapRDD.top(3, new TupleComparator2());
            System.out.println(collect);
        }
    }
}