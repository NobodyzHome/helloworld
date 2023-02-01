package com.mzq.hello.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaUsage {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 100001; i <= 10000000; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("hello_world", "hello" + i, "hello world" + i);
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();
    }
}
