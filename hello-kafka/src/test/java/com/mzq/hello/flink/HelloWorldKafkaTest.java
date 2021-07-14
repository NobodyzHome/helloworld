package com.mzq.hello.flink;

import com.mzq.hello.domain.WaybillRouteLink;
import com.mzq.hello.util.GenerateDomainUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

public class HelloWorldKafkaTest {

    @Test
    public void testProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "my-client");

        KafkaProducer<String, WaybillRouteLink> kafkaProducer = new KafkaProducer<>(properties);
        WaybillRouteLink waybillRouteLink = GenerateDomainUtils.generateWaybillRouteLink();

        ProducerRecord<String, WaybillRouteLink> producerRecord = new ProducerRecord<>("hello-world", waybillRouteLink.getWaybillCode(), waybillRouteLink);
        Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
        kafkaProducer.close();
    }

    @Test
    public void testConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WaybillRouteDeSerializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "testClient");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        KafkaConsumer<String, WaybillRouteLink> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));
        ConsumerRecords<String, WaybillRouteLink> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(5));
        consumerRecords.forEach(record -> System.out.println(record.timestamp()));

        kafkaConsumer.close();
    }

    @Test
    public void testConsumer1() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "testClient");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WaybillRouteDeSerializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        KafkaConsumer<String, WaybillRouteLink> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton("hello-world"));


        Set<TopicPartition> assignment;
        do {
            kafkaConsumer.poll(Duration.ofMillis(100));
            assignment = kafkaConsumer.assignment();
        } while (assignment.isEmpty());

        kafkaConsumer.seekToBeginning(assignment);
        ConsumerRecords<String, WaybillRouteLink> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(2000));
        System.out.println(consumerRecords);
        kafkaConsumer.close();
    }
}
