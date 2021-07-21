package com.mzq.hello.flink;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ProducerTest {

    @Test
    public void test1() throws ExecutionException, InterruptedException, TimeoutException {
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG,"producer-client");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"3000000");
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> producerRecord=new ProducerRecord<>("hello-world","zhangsan","test-lisi");
        /*
        * 一个分区可以有多个副本，其中只有一个是leader副本，其他都是follower副本。同时，kafka要求在同一个broker中，只能存放一个partition的一个副本（可以是leader副本，也可以是follower副本），因此leader副本和follower副本肯定不在一个broker中。
        * 生产客户端只会将数据发送到对应分区的leader副本，其他broker会定时将leader副本中的数据同步到follower副本中。由于网络等因素，多个broker将leader副本的数据同步到follower副本的效率也是不同的，因此产生了以下概念：
        * 【ISR】:leader副本和那些与leader副本中的数据差不多follower副本（同步效率比较好的），我们称之为ISR（in sync replicas）。
        * 【OSR】:与ISR对应的，那些与leader副本中数据查很多的follower副本，我们称之为OSR(out of sync replicas)而我们将一个分区的全部副本。
        * 【AR】:leader副本以及所有的follower副本组成了AR（assigned replicas）
        * AR = ISR + OSR
        * leader副本负责跟踪和维护ISR，如果一个ISR中的follower突然与leader差了很多数据，那么leader副本会将它踢出ISR中；如果一个OSR中的follower突然与leader的数据追平了，那么leader副本会将它加入到ISR。
        * 当leader副本宕机后，kafka会从ISR中选出新的leader副本，而OSR中的follower则没有机会
        *
        * 1.向kafka server发送metaDataRequest，获取topic的元数据
        * 2.将数据经由serializer进行序列化
        * 3.将数据经由Partitioner，获取需要发送的分区
        */
        Future<RecordMetadata> send = kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                System.out.println("test");
            }
        });
        RecordMetadata recordMetadata = send.get();
        System.out.println(recordMetadata);

        Thread.sleep(5000);

        kafkaProducer.close();
    }
}
