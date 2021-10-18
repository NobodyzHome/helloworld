package com.mzq.hello.flink;

import com.mzq.hello.domain.BdWaybillOrder;
import com.mzq.hello.util.GenerateDomainUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class ConsumerTest {

    public static final String TOPIC = "bd-waybill-info";

    @Slf4j
    public static class BdWaybillInterceptor implements ProducerInterceptor<String, BdWaybillOrder> {
        @Override
        public ProducerRecord<String, BdWaybillOrder> onSend(ProducerRecord<String, BdWaybillOrder> record) {
            log.info("即将发送数据。topic={},key={},value={}", record.topic(), record.key(), record.value());
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if (Objects.nonNull(exception)) {
                log.error("发送数据失败！", exception);
            } else {
                log.info("发送数据成功！topic={},partition={},offset={},timestamp={}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            }
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

    @Test
    public void sendRecords() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(20).toBytes()));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(50).toBytes()));
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DataSize.ofMegabytes(10).toBytes()));
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Duration.ofSeconds(3).toMillis()));
        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, BdWaybillInterceptor.class.getName());
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(3).toMillis()));
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(5).toMillis()));

        KafkaProducer<String, BdWaybillOrder> kafkaProducer = new KafkaProducer<>(properties);
        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(1000);
//        String waybillCode = bdWaybillOrders.get(0).getWaybillCode();
//        bdWaybillOrders.forEach(order -> order.setWaybillCode(waybillCode));

        for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
            ProducerRecord<String, BdWaybillOrder> producerRecord = new ProducerRecord<>(TOPIC, bdWaybillOrder.getWaybillCode(), bdWaybillOrder);
            kafkaProducer.send(producerRecord);
        }

        kafkaProducer.close();
    }

    /**
     * 目标方法：
     * 1.subscribe
     * 2.poll
     */
    @Test
    public void testSubscribe() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello-group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-client");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(1).toMillis()));
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(5).toMillis()));
        // 两次poll的最大间隔，如果超出该间隔，consumer则会向broker发出退出group的请求，broker就会剥夺分配给它的partition，然后分配给其他的client
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(30).toMillis()));

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        /*
         * 订阅模式的话，当前客户端会加入到一个group中，broker会根据group中已有的client数和订阅的topic的partition数来决定给当前客户端分配几个partition。
         * 在这个过程中可能会剥夺该组中其他client已分配的某些partition，然后将剥夺出来的partition分配给当前client，用于平均分配每个client的partition，也就是平均分配每个client的负载，我们把这种称之为再平衡。
         *
         * 注意：这个方法执行时是不会和broker有交互的，需要到poll方法执行时才会和broker有交互
         */
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));
        // 从broker中拉取数据，当为当前consumer分配的分区都没有数据时，用户线程会阻塞，等到数据的到来。阻塞时长为入参的Duration。
        ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5000));
        // 我们可以直接遍历拉取到的每一条数据
        for (ConsumerRecord<String, BdWaybillOrder> consumerRecord : consumerRecords) {
            log.info("topic={},key={},value={},partition={},offset={},timestamp={}", consumerRecord.topic()
                    , consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.timestamp());
        }
        // 我们也可以按照分区来遍历数据，这点在手动提交位点时非常重要
        // 获取当前拉取到的数据中，都有哪些分区
        Set<TopicPartition> partitions = consumerRecords.partitions();
        for (TopicPartition topicPartition : partitions) {
            // 获取当前遍历的分区的所有数据
            List<ConsumerRecord<String, BdWaybillOrder>> partitionRecords = consumerRecords.records(topicPartition);
            log.info("==============partition:{}==============", topicPartition.partition());
            for (ConsumerRecord<String, BdWaybillOrder> consumerRecord : partitionRecords) {
                log.info("topic={},partition={},offset={},key={}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key());
            }
        }
        // KafkaConsumer使用完毕后一定要记得关闭，断开程序和broker的连接
        kafkaConsumer.close();
    }

    /**
     * 上面说了，当我们subscribe一个topic时，broker会给我们分配一些partition，我们的client就可以拉取这些partition的数据。但这不代表我们的client就可以一直拥有这些partion。
     * 当group中出现了新的client，为了各个broker负载可以达到均衡，broker会剥夺一些已有client的partition，然后将这些partition分配给新的client。
     * 同样的，当group中某个client断开了连接，broker也会把为断开连接所分配的partition分配给其他已有的client上，这样已有的client就多了分配的partition。
     * 但这个过程，如果我们的client只是使用subscribe(topic)，那么在发生再平衡时，当前client是对broker剥夺了它的partition或给它增加了新的partition毫不知情的，它依然是利用poll来拉取数据，只不过拉取的partition有可能随着再平衡的发生而增加或减少了。
     * 其实我们也是可以知道再均衡发生时，我们的客户端增加或减少了哪些partition的。就是使用subscribe(topic,ConsumerRebalanceListener)这种形式。
     */
    @Test
    public void testRebalance() throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello-group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-client");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("移出了以下分区：" + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("增加了以下分区：" + partitions);
            }
        });

        /*
         * 假设group中有consumerA、consumerB、consumerC：
         * 1.当加入一个consumerD时，broker就会发起再平衡，在再平衡期间，broker会收回所有已经给consumer分配的partition，此时需要consumerA、B、C都发起了join group请求（poll方法）或leave group请求（close方法），
         *   broker才会给存活的consumer（在这里可能是consumerA、consumerB、consumerC、consumerD）平均分配partition，这样就完成了再平衡。
         * 2.当consumerC发送leave group请求时，broker也会发起再平衡，在再平衡期间，broker会收回所有已经给consumer分配的partition，此时需要consumerA、B发起join group请求或leave group请求，broker才会进行partition的重新分配
         *   （在这里可能是consumerA、consumerB），这样就完成了再平衡。
         */
        ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5000));
        System.out.println(consumerRecords);
        ConsumerRecords<String, BdWaybillOrder> consumerRecords1 = kafkaConsumer.poll(Duration.ofMillis(5000));
        ConsumerRecords<String, BdWaybillOrder> consumerRecords2 = kafkaConsumer.poll(Duration.ofMillis(5000));
        // 假如当前consumer只获取到了partition3和4，当前consumer也可以对他没拥有的partition进行commit位点，broker会短暂地把那个partition的位点改变，但当他发现改变位点的consumer并不拥有该分区时，它就把位点信息又调回改变之前的了
        // 所以consumer在提交位点时，只对分配给它的分区提交位点，才是有效的。对不属于它的分区进行位点提交，即使提交了也是无效的。
        kafkaConsumer.commitSync(Collections.singletonMap(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1L)));
        kafkaConsumer.close();
    }

    /**
     * 1.partitionsFor
     * 2.assign
     */
    @Test
    public void testAssign() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "test-client");

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        // 调用partitionsFor方法，和broker进行交互，获取指定topic都有哪些partition
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(TOPIC);
        List<PartitionInfo> partitionsToAssign = partitionInfos.stream().filter(partition -> partition.partition() <= 2).collect(Collectors.toList());
        // 使用assign方法订阅指定partition
        kafkaConsumer.assign(partitionsToAssign.stream().map(partition -> new TopicPartition(partition.topic(), partition.partition())).collect(Collectors.toList()));

        int count = 1;
        while (count++ <= 100) {
            /*
             * 由于使用的是assign方式，所以消费者没有group的概念，因此也就没有当前分组针对各个partition有多少数据没消费（lag）的概念，所以
             * assign方式能拉取到的数据是只有在consumer和broker建立起连接后，producer新往对应partition发送的数据才可以被消费到。
             */
            ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(2000));
            System.out.println(consumerRecords);
        }
        kafkaConsumer.close();

    }

    /**
     * 1.position
     * 2.committed
     * 3.beginningOffsets
     * 4.endOffsets
     * 5.offsetsForTimes
     * <p>
     * 总结：
     * beginningOffsets、endOffsets、offsetsForTimes：他们查询的都是在broker的partition中实际数据对应的位移信息。查询时需要：topic、partition、time（仅在offsetsForTimes时使用）
     * committed：查询的是broker存储于__consumer_offsets中的，指定group针对各个分区的位移。查询时需要：group信息、topic、partition。
     * position：用于让当前consumer查询分配给他的partition，下次拉取的位移，所以该方法只能在获取到partition后才能调用。查询时需要：group信息、topic、partition（这里的topic和partition必须是分配给这个consumer的topic和partition）
     */
    @Test
    public void testFindOffset() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello-group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "hello-client");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);

        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(TOPIC);
        // 查询broker中存储的__consumer_offsets中，针对当前consumer所在的group以及指定topic和partition中对应的位点位置（这个位置是下次拉取时的起始位置），它随着consumer进行commit而增加
        // 注意：在使用该方法时，当前consumer可以没从broker中获取到分配的partition，但是当前consumer必须设置group.id，用于查询哪个group的已提交位点信息
        Set<TopicPartition> topicPartitionSet = partitionInfos.stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toSet());
        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(topicPartitionSet);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            log.info("partition={},committed={}", topicPartition.partition(), offsetAndMetadata.offset());
        }

        // 查询指定topic和partition对应的在broker中存储的起始的offset，它随着broker定时对partition的历史数据的删除而改变
        // 同样地，该方法也不需要在consumer获取到partition时才能调用
        Map<TopicPartition, Long> beginningOffsets = kafkaConsumer.beginningOffsets(topicPartitionSet);
        for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
            int partition = entry.getKey().partition();
            Long beginningOffset = entry.getValue();
            log.info("partition={},beginningOffset={}", partition, beginningOffset);
        }

        // 查询指定topic和partition在broker中存储的结束offset+1（也就是LEO），它随着producer往该分区里写入数据而改变
        // 同样地，该方法也不需要在consumer获取到partition时才能调用
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(topicPartitionSet);
        for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
            int partition = entry.getKey().partition();
            Long endOffset = entry.getValue();
            log.info("partition={},endOffset={}", partition, endOffset);
        }

         /*
            该方法可以根据指定分区的查询时间，查询broker存储的分区数据中，最早一条在指定时间之后的数据的offset。说到时间，这里就有个疑问了，broker怎么知道每条数据的时间？我们在使用producer发送数据时，也没有指定这条数据的时间呀（通过new Producer(topic,key,value)的方式创建的数据）
            实际上KafkaProducer在send ProducerRecord时，如果发现timestamp属性没有被赋值，那么它会使用System.currentTimeMillis()来作为这条数据的时间戳，因此如果我们不指定数据的时间时，那么发送这条数据的时间就是这条数据的时间戳
            官方对ProducerRecord中timestamp属性的说明：The timestamp of the record, in milliseconds since epoch. If null, the producer will assign the timestamp using System.currentTimeMillis().
            同样地，该方法也不需要在consumer获取到partition时才能调用
        */
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2021-08-04T00:00:00+08:00");
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = kafkaConsumer.offsetsForTimes(partitionInfos.stream().collect(Collectors.toMap(info -> new TopicPartition(info.topic(), info.partition()), info -> zonedDateTime.toInstant().toEpochMilli())));
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
            int partition = entry.getKey().partition();
            long offset = entry.getValue().offset();
            log.info("partition={},queryDate={},offset={}", partition, zonedDateTime, offset);
        }

//        for (TopicPartition topicPartition : topicPartitionSet) {
//            // position方法用于查询当前consumer下一次拉取offset，它随着consumer调用poll方法而增加。注意：consumer只能查询分配给它的partition，否则会报错：You can only check the position for partitions assigned to this consumer.
//            long position = kafkaConsumer.position(topicPartition);
//            log.info("partition={},position={}", topicPartition.partition(), position);
//        }

        kafkaConsumer.subscribe(Collections.singleton(TOPIC));
        int count = 1, max = 3;

        do {
            ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(300));
            if (Objects.nonNull(consumerRecords) && !consumerRecords.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>(consumerRecords.partitions().size());
                Set<TopicPartition> assignment = kafkaConsumer.assignment();
                for (TopicPartition assignTopicPartition : assignment) {
                    long position = kafkaConsumer.position(assignTopicPartition);
                    log.info("position.partition={},position={}", assignTopicPartition.partition(), position);
                }

                Set<TopicPartition> partitions = consumerRecords.partitions();
                for (TopicPartition topicPartition : partitions) {
                    List<ConsumerRecord<String, BdWaybillOrder>> records = consumerRecords.records(topicPartition);
//                    for (ConsumerRecord<String, BdWaybillOrder> consumerRecord : records) {
//                        log.info("consumed.partition={},offset={}", consumerRecord.partition(), consumerRecord.offset());
//                    }
                    ConsumerRecord<String, BdWaybillOrder> lastConsumerRecord = records.get(records.size() - 1);
                    ConsumerRecord<String, BdWaybillOrder> firstConsumerRecord = records.get(0);
                    log.info("partition={},firstRecordOffset={},lastRecordOffset={}", lastConsumerRecord.partition(), firstConsumerRecord.offset(), lastConsumerRecord.offset());
                    toCommit.put(topicPartition, new OffsetAndMetadata(lastConsumerRecord.offset() + 1));
                }
                kafkaConsumer.commitSync(toCommit);

                Map<TopicPartition, OffsetAndMetadata> committed1 = kafkaConsumer.committed(partitions);
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed1.entrySet()) {
                    TopicPartition topicPartition = entry.getKey();
                    OffsetAndMetadata offsetAndMetadata = entry.getValue();
                    log.info("partition={},committed={}", topicPartition.partition(), offsetAndMetadata.offset());
                }
                count++;
            }
        } while (count <= max);
    }


    /**
     * 1.assignment
     * 只能对分配给客户端的partition的offset进行修改
     * 2.seek
     * 3.seekToBeginning
     * 4.seekToEnd
     */
    @Test
    public void testSeek() {
        // 我们在上面的例子中，使用了各种方法来获取各种位点信息，为了什么？就是为了和seek方法联动，只要你知道要拉取的位点信息，就可以使用seek方法，可以把consumer拉取的位点移动到对应位置
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "myClient");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeSerializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        Set<TopicPartition> assignment;
        do {
            kafkaConsumer.poll(Duration.ofMillis(200));
            assignment = kafkaConsumer.assignment();
        } while (Objects.isNull(assignment) || assignment.isEmpty());

        Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(assignment);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : committed.entrySet()) {
            kafkaConsumer.seek(offset.getKey(), offset.getValue().offset() - 10);
        }

        int curTime = 1, maxTimes = 10;
        do {
            ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(800));
            Set<TopicPartition> partitions = consumerRecords.partitions();
            for (TopicPartition topicPartition : partitions) {
                List<ConsumerRecord<String, BdWaybillOrder>> partitionRecords = consumerRecords.records(topicPartition);
                long startPosition = partitionRecords.get(0).offset();
                long endPosition = partitionRecords.get(partitionRecords.size() - 1).offset();
                long position = kafkaConsumer.position(topicPartition);
            }
        } while (++curTime <= maxTimes);

        kafkaConsumer.close();
    }

    @Test
    public void testSeekBeginning() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "myClient");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        Set<TopicPartition> assignment;
        do {
            kafkaConsumer.poll(Duration.ofMillis(300));
            assignment = kafkaConsumer.assignment();
        } while (assignment.isEmpty());

        kafkaConsumer.seekToBeginning(assignment);
        for (TopicPartition topicPartition : assignment) {
            long position = kafkaConsumer.position(topicPartition);
        }

        int curTime = 1, maxTime = 5;
        do {
            ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(800));
            for (TopicPartition topicPartition : consumerRecords.partitions()) {
                for (ConsumerRecord<String, BdWaybillOrder> consumerRecord : consumerRecords.records(topicPartition)) {
                    log.info("key={},value={},partition={},offset={}", consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset());
                }
            }
        } while (++curTime <= maxTime);
        kafkaConsumer.close();
    }

    @Test
    public void testCommitSync() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello-group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-client");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 每个分区最大拉取的数据数量，如果拉取一个分区中，第一条数据的大小就超过了该配置，那consumer就不会继续从该分区再拉取数据了
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(DataSize.ofKilobytes(3).toBytes()));
        // 本次poll请求最大拉取的数据量，也就是说从所有分配给这个consumer的分区中拉取到的所有数据的数据量的最大值
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(DataSize.ofKilobytes(20).toBytes()));
        // 本次poll请求最大拉取的数据条数。它和fetch.max.bytes是谁先到达就用谁的关系，例如fetch.max.bytes配置为100MB，但max.poll.records配置为10，那么consumer依然会只最多拉取10条数据，反之亦然。
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        // consumer两次调用poll()方法拉取数据，如果超过了该间隔，当前consumer会给broker发送一个leave group request，这样broker就会把当前consumer剔除出group，将分配给它的分区分配给组内其他正在保持心跳的consumer
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(5).toMillis()));
        // 一次poll请求最少拉取的数据量，如果broker中数据量少于该配置，那么broker不会马上对这个POLL REQUEST进行响应，而是暂时挂起当前请求，直到broker中收到足够的数据后才会给当前REQUEST进行响应
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(DataSize.ofBytes(1).toBytes()));
        // 由于fetch.min.bytes可能使当前poll request进行挂起等待，为了避免长时间让consumer进行等待，可以配置该参数，这样broker会有两个条件来对挂起的request进行响应：1.broker中有足够数量的数据了 2.broker判断挂起时长超过该配置了
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        Set<TopicPartition> assignment;
        do {
            kafkaConsumer.poll(Duration.ofMillis(300));
            assignment = kafkaConsumer.assignment();
        } while (assignment.isEmpty());

        long searchTime = ZonedDateTime.parse("2021-12-01T00:00:00+08:00").toInstant().toEpochMilli();
        // 如果offsetsForTimes没有查询到对应分区在指定时间之后的数据，那么返回的map中，该分区对应的value是null，因此要注意null的处理
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsets = kafkaConsumer.offsetsForTimes(assignment.stream().collect(Collectors.toMap(topicPartition -> topicPartition, topicPartition -> searchTime)));
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> topicPartitionOffset : topicPartitionOffsets.entrySet()) {
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffset.getValue();
            long offset;
            TopicPartition topicPartition = topicPartitionOffset.getKey();
            if (Objects.nonNull(offsetAndTimestamp)) {
                offset = offsetAndTimestamp.offset();
            } else {
                Map<TopicPartition, Long> beginningOffsets = kafkaConsumer.beginningOffsets(Collections.singleton(topicPartition));
                offset = beginningOffsets.get(topicPartition);
            }
            kafkaConsumer.seek(topicPartition, offset);
        }

        int curTime = 1, maxTime = 5;
        do {
            ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(800));
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>(consumerRecords.partitions().size());
            for (TopicPartition topicPartition : consumerRecords.partitions()) {
                long lastOffset = -1;
                for (ConsumerRecord<String, BdWaybillOrder> consumerRecord : consumerRecords.records(topicPartition)) {
                    log.info("key={},value={},partition={},offset={}", consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset());
                    lastOffset = consumerRecord.offset();
                }
                toCommit.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));

                /*
                1.为什么要手动提交位点，因为自动提交位点的话，在拉取数据指定时间之后，consumer就认为数据处理正确了，可以提交位点了，但真正这批数据拉取完以后处理的正不正确，只有用户程序知道。所以自动提交位点有可能产生的问题是位点提交了，
                  但是这批拉取的数据处理异常，但是由于位点已经提交，无法再次拉取到处理错误的数据了（除非consumer主动使用seek）。所以我们在处理位点时，还是应尽量使用手动提交，在拉取的数据正确处理后再提交位点。
                2.每次拉取并处理完数据时就进行一次commit，为什么要这样做？假如多次拉取，最后一起commit，有可能造成的问题是假设前两次拉取数据和处理数据没问题，而第三次拉取和处理数据出现异常了，导致位点提交没有执行，这样这三次拉取的数据都白处理了
                  ，下次再拉取数据还是得从第一次拉取的位置拉取数据（因为没有提交位点），这就导致了数据的重复处理
                3.每次拉取并处理完数据时进行一次commit，那么仅有可能在出现异常时，让这次拉取的数据白处理了，也就是只重复处理了这次拉取的数据。
                4.位点提交也要注意提交的频率，因为每一次提交都需要和kafka进行通信，并且是阻塞的（因为是同步提交），在kafka没有对提交位点操作给出回应之前，用户线程都处于阻塞状态。
                  在这里我们又细分到拉取一次数据后，按数据的partition进行位点提交，每有一个partition就提交一次。这应该只用于测试，正常情况下
                5.同步提交位点适用于客户端需要知道位点提交结果的情况
                */
                kafkaConsumer.commitSync(toCommit);
            }


            // commitSync方法可以把当前consumer所拉到的所有分区的位点一起提交。该方法相对来说卡的就比较死，我们不能指定每个分区的LEO
//            kafkaConsumer.commitSync();
        } while (++curTime <= maxTime);
        kafkaConsumer.close();
    }

    @Test
    public void testCommitAsync() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-client");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "800");
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1500");
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(DataSize.ofKilobytes(2).toBytes()));
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(DataSize.ofKilobytes(5).toBytes()));
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "30");
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "2000");
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(DataSize.ofKilobytes(1).toBytes()));
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "900");

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        Set<TopicPartition> assignment;
        do {
            kafkaConsumer.poll(Duration.ofMillis(300));
            assignment = kafkaConsumer.assignment();
        } while (assignment.isEmpty());

        kafkaConsumer.seekToBeginning(assignment);

        int currentTime = 1, maxTime = 5;
        do {
            ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>(consumerRecords.partitions().size());
            for (TopicPartition topicPartition : consumerRecords.partitions()) {
                long lastOffset = 0;
                for (ConsumerRecord<String, BdWaybillOrder> consumerRecord : consumerRecords.records(topicPartition)) {
                    log.info("consumer record.key={},value={},partition={},offset={}", consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset());
                    lastOffset = consumerRecord.offset();
                }
                toCommit.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
            }

            // 和同步提交位点一样，异步提交位点也是用于更精准控制位点何时真正应该提交（在数据都处理完），只不过和同步提交位点不同的是，异步提交位点不会阻塞用户线程，通常用于用户不需要关心位点提交结果的情况下
            // commitAsync方法中传入的callback对象，会在broker对请求响应后，客户端任意线程调用poll方法时执行callback对象
            kafkaConsumer.commitAsync(toCommit, (offsets, exception) -> {
                if (Objects.nonNull(exception)) {
                    log.error("异步提交位点异常！", exception);
                } else {
                    log.info("异步提交位点成功。位点信息：{}", offsets);
                }

            });
        } while (++currentTime <= maxTime);
        kafkaConsumer.close();
    }
}
