package com.mzq.usage.hadoop.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * 关于Driver：
 * 用于创建SparkContext、然后使用SparkContext生成RDD、最后调用RDD的action提交spark任务的类，我们称之为Driver。
 * 一个Driver必须要有main方法，用于执行上面说的这些操作。因此这个HelloSpark就是一个Driver。
 * 由于提交方式的不同，调用Driver的main方法的地方也是不同的。当master=yarn-client时，是在本地jvm中执行Driver的main方法，
 * 而当master=yarn-cluster时，是在该任务的ApplicationMaster(在NodeManager上)来执行Driver的main方法的。
 */
@Slf4j
public class HelloSpark {

    /**
     * 通过下面的描述可以看到，spark的Driver和MapReduce的客户端有很大的区别：
     * 1.工作职责不同。Driver在spark任务中起到了非常重要的作用，不仅负责job的提交，还负责任务调度、任务执行结果的收集等。而MapReduce的客户端只负责任务的提交，把任务提交到ResourceManager后，就没有MapReduce客户端的事儿了。
     * 2.运行生命周期不同。Driver需要在Job执行时的每一个时刻都保持可用，因为他要监控任务的执行情况。而MapReduce的客户端在提交完任务后就可以停止了。
     * 3.通信范围不同。Driver需要和ResourceManager、NodeManager上的ApplicationMaster、NodeManager上的Executor进行通信。而MapReduce的客户端只和ResourceManager通信
     * 4.获取任务执行结果的方式不同。Driver可以在程序中获取任务的执行结果，而MapReduce客户端不能获取任务的执行结果，只能从设置的output文件中获取执行结果
     * <p>
     * 由此可见，spark的Driver远比MapReduce的客户端的作用要高很多。因此，更建议把Driver运行在安全的地方，也就是建议运行在yarn的NodeManager上（即yarn-cluster的执行方式），而不是直接跑在一台客户的机器中（即yarn-client的执行方式）。
     */
    public static void main(String[] args) {
        // 从配置文件中获取参数，然后赋值到系统参数中，为SparkConf做准备
        Properties properties = new Properties();
        URL resource = HelloSpark.class.getClassLoader().getResource("spark-site.properties");
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
        sparkConf.setMaster("spark://localhost:7077");
        // 设置任务的名称
        sparkConf.setAppName("hello_world");
        // 设置程序包的位置，和mapreduce一样，为了在nodemanager上执行程序，因此需要把打好包的程序包
        sparkConf.setJars(new String[]{"hello-hadoop/target/hello-hadoop-1.0-SNAPSHOT.jar"});
        /*
            在创建JavaSparkContext对象时，会：
            1.生成配置文件，包括spark job相关的配置文件__spark_conf__.properties，hadoop相关的配置文件__spark_hadoop_conf__.xml
            2.将配置文件、spark依赖的jar包、任务的程序包上传到hdfs。上传到hdfs的/user/maziqiang/.sparkStaging/application_1652080053984_0006目录下，其中application_1652080053984_0006为任务名称
            3.创建YarnSchedulerBackend，用于向yarn发起请求以及接收yarn发来的请求。创建完毕后使用YarnSchedulerBackend向ResourceManager提交应用(submitApplication)
            4.ResourceManager在收到提交来的应用后，会找一个不忙的NodeManager，向其发出创建container请求，请求中执行的command是bin/java org.apache.spark.deploy.yarn.ApplicationMaster
            5.NodeManager接到创建container的请求后，会执行对应的command，也就是启动一个JVM，然后执行ApplicationMaster类。ApplicationMaster类在执行时，会先向Driver发起请求RegisterClusterManager请求，告知Driver，AM已创建完毕
            6.Driver上的YarnSchedulerBackend在收到RegisterClusterManager请求后，会进行一些初始化工作，但不会对该请求做出响应
            7.ApplicationMaster在通知Driver后，会拿到从hdfs中拉取下来的spark job的配置文件__spark_conf，从中获取该job需要的Executor数量，然后向ResourceManager申请对应数量的资源
            8.ResourceManager在收到资源申请的请求后，会根据整个集群的资源使用情况，给ApplicationMaster分配对应数量的NodeManager
            9.ApplicationMaster收到ResourceManager分配的资源后，会向对应NodeManager发起创建container请求，请求中执行的command是bin/java org.apache.spark.executor.CoarseGrainedExecutorBackend(参阅org.apache.spark.deploy.yarn.ExecutorRunnable类)
            10.NodeManager接到创建container的请求后，会执行对应的command，也就是启动一个JVM，然后执行CoarseGrainedExecutorBackend类。CoarseGrainedExecutorBackend类在执行时，会向Driver发起RegisterExecutor请求，向Driver注册该Executor
            11.Driver上的YarnSchedulerBackend收到CoarseGrainedExecutorBackend发来的RegisterExecutor请求后，会响应RegisteredExecutor，代表可以创建该Executor
            12.当NodeManager上的CoarseGrainedExecutorBackend收到Driver发送来的RegisteredExecutor响应后，则会开始创建Executor，准备接收SubTask
            13.Driver会记录每一个注册过来且被允许创建的Executor，待调用RDD的action方法后，将SubTask发送至Executor中
         */
        JavaSparkContext sp = new JavaSparkContext(sparkConf);
        // 执行此行时，只是在本地内存中创建RDD对象
        JavaRDD<Integer> rDD = sp.parallelize(Arrays.asList(1, 2, 3, 4)).map(num -> num + 1).filter(num -> num > 3);
        /*
            执行RDD的collect方法后，SparkContext就会开始进行拓扑的拆分以及任务的调度了：
            1.根据RDD描述的DAG，将整个任务进行切分多个Stage
            2.将每个Stage切分成多个SubTask
            3.将每个SubTask序列化，然后创建LaunchTask请求，发送到注册过来的NodeManager的CoarseGrainedExecutorBackend
            4.在NodeManager中的CoarseGrainedExecutorBackend接收到LaunchTask请求后，则会对SubTask进行反序列化，然后交由Executor进行SubTask的执行
            5.等待job执行完毕，然后接收Executor返回的整个job的执行结果

            从这里我们可以看到，我们完全可以通过java程序，向spark或yarn集群提交一个yarn任务，然后一直等待收集任务执行完毕后的结果
        */
        List<Integer> collect = rDD.collect();
        log.info("执行结果：{}", collect);
         /*
            执行完毕后，需要关闭JavaSparkContext，此时会：
            1.向注册过来的所有NodeManager的CoarseGrainedExecutorBackend发送StopExecutor请求，进行Executor的关闭
            2.NodeManager的CoarseGrainedExecutorBackend收到StopExecutor请求后，关闭Executor
            3.断开和ApplicationMaster的连接
            4.删除在hdfs中存储的该job的spark依赖包、任务程序包、配置文件等内容
            5.ApplicationMaster在发现Driver已断开连接后，将job的状态设置为finished和success，同时ApplicationMaster向ResourceManager发起应用注销
         */
        sp.close();
    }
}
