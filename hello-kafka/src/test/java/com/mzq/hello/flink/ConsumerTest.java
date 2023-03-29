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

/**
 * 拉取数据流程中，需要先订阅并且调用poll方法获取到partition后，才能拉取数据
 * 提交位点时，可以不订阅topic，只需要给出group.id，就可以为指定group.id来提交位点
 */
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
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "100000");

        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        /*
         * ConsumerRebalanceListener主要用于这个场景：假设我当前consumer poll了很多次，但是还没有提交，这时，如果当前consumer poll超时了，那么就会把分给当前consumer的分区剥夺。
         * 由于当前consumer并没有提交位点，因此接到这个分区的consumer又要重新从上次提交位点的数据开始处理，导致这个consumer处理了很多当前consumer已处理过的数据。
         * 假如当前consumer发现被从group移除后，又重新加入group，那么当前consumer又被重新分配了这些分区，这时ConsumerRebalanceListener就起作用了，在onPartitionsAssigned方法执行时，
         * 发现如果有此前没来得及提交分区的位点信息，那么就使用seek，直接将拉取的位点信息移动至上次被剥夺分区前已处理的位点信息。这样，当前consumer就在被剥夺分区然后重新获取分区后，不会重复处理已处理过但没来得及提交的数据了
         */
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {

            /**
             * 当前方法会在当前consumer被剥夺分区后，下一次调用poll方法时来执行该方法
             * 执行到该方法时，broker就已经剥夺了入参的这些分区，所以无法使用kafkaConsumer做一些必须拥有分区后才能执行的方法，例如seek、commit、position方法
             *
             * @param partitions 已剥夺的分区
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("移出了以下分区：" + partitions);
            }

            /**
             * 当前方法会在当前consumer调用poll方法时，发送JoinGroupRequest成功并且broker给它分配分区后，来执行该方法
             * 执行到该方法时，broker就已经分配给该consumer分区了
             *
             * @param partitions 分配给的分区
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("增加了以下分区：" + partitions);
                // 如果有未提交的位点信息记录，那么把位点移动至未提交的位点位置，代表之前处理过的数据就不要再重复处理了
                for (Map.Entry<TopicPartition, Long> entry : offsetMap.entrySet()) {
                    log.info("检测到分区有此前未提交的位点信息，现在把位点信息拉取到该位置。分区={}，拉取到的位点信息={}", entry.getKey(), entry.getValue() + 1);
                    kafkaConsumer.seek(entry.getKey(), entry.getValue() + 1);
                }
                for (TopicPartition topicPartition : partitions) {
                    long position = kafkaConsumer.position(topicPartition);
                    log.info("分区【{}】的初始拉取位置：{}", topicPartition, position);
                }
            }
        };
        kafkaConsumer.subscribe(Collections.singleton(TOPIC), listener);

        Set<TopicPartition> assignment;
        do {
            kafkaConsumer.poll(Duration.ofMillis(300));
            assignment = kafkaConsumer.assignment();
        } while (Objects.isNull(assignment) || assignment.isEmpty());

        Map<TopicPartition, Long> beginningOffsets = kafkaConsumer.beginningOffsets(assignment);
        for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
            kafkaConsumer.seek(entry.getKey(), entry.getValue());
        }

        /*
         * 假设group中有consumerA、consumerB、consumerC：
         * 1.当加入一个consumerD时，broker就会发起再平衡，在再平衡期间，broker会收回所有已经给consumer分配的partition，此时需要consumerA、B、C都发起了join group请求（poll方法）或leave group请求（close方法），
         *   broker才会给存活的consumer（在这里可能是consumerA、consumerB、consumerC、consumerD）平均分配partition，这样就完成了再平衡。
         * 2.当consumerC发送leave group请求时，broker也会发起再平衡，在再平衡期间，broker会收回所有已经给consumer分配的partition，此时需要consumerA、B发起join group请求或leave group请求，broker才会进行partition的重新分配
         *   （在这里可能是consumerA、consumerB），这样就完成了再平衡。
         * 3.ConsumerRebalanceListener的onPartitionsRevoked、onPartitionsAssigned方法都是在调用poll方法时执行的，具体来说是client向broker发送LeaveGroupRequest、JoinGroupRequest请求得到响应后执行的。
         *   我们如果通过手动提交位点的方式提交位点，那么上一次poll拉取的数据有可能没有提交位点，这时我们就需要记录每个分区需要提交的位点信息，当当前consumer被剥夺分区然后再次获取到该分区后，可以在onPartitionsAssigned中把位点信息调回来
         * 4.同时它也能处理，假设当前consumer被剥夺分区后，如果把分区分配给其他consumer，并且其他consumer对该分区提交了位点。当这个consumer退出后，broker将分区归还给当前consumer后，当前consumer可以将位点还原回它上次处理的位置，而不是从使用其他consumer已提交的位点位置。
         */
        ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5000));
        for (TopicPartition topicPartition : consumerRecords.partitions()) {
            List<ConsumerRecord<String, BdWaybillOrder>> partitionRecords = consumerRecords.records(topicPartition);
            ConsumerRecord<String, BdWaybillOrder> consumerRecord = partitionRecords.get(partitionRecords.size() - 1);
            log.info("分区{}拉取到最后一条数据的offset:{}", topicPartition, consumerRecord.offset());
            offsetMap.put(topicPartition, consumerRecord.offset());
        }

        // 让主线程睡6秒，在此期间kafka的心跳线程发现距离上一次poll方法执行的时间已经超过了max.poll.interval.ms的配置，那么kafka的子线程就会发起LeaveGroupRequest，broker就会把当前consumer的分区进行剥夺
        Thread.sleep(6000);
        // 主线程再一次调用poll方法后，consumer发现已经被剥夺分区了，就会重新向GroupCoordinator发起JoinGroupRequest，broker就会把分区重新分配给它。
        // 这里就是关键点，我们要在重新获取到分区后，把上一次poll拉取数据的位点信息（没来得及提交到kafka中）重新恢复到当前consumer中，使当前consumer不要再重复处理上一次poll时已处理过的数据
        ConsumerRecords<String, BdWaybillOrder> consumerRecords1;
        do {
            consumerRecords1 = kafkaConsumer.poll(Duration.ofMillis(3000));
            if (!consumerRecords1.isEmpty()) {
                for (TopicPartition topicPartition : consumerRecords1.partitions()) {
                    List<ConsumerRecord<String, BdWaybillOrder>> records = consumerRecords1.records(topicPartition);
                    ConsumerRecord<String, BdWaybillOrder> firstRecord = records.get(0);
                    ConsumerRecord<String, BdWaybillOrder> lastRecord = records.get(records.size() - 1);
                    log.info("分区{}的第一条数据的offset:{}，最后一条数据的offset:{}", topicPartition, firstRecord.offset(), lastRecord.offset());
                    // 每一个分区提交一次位点
                    kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastRecord.offset() + 1)));
                }
            }
        } while (consumerRecords1.isEmpty());

        kafkaConsumer.close();
        System.out.println("test");
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
            // consumer只能对broker分配给它的分区进行seek操作，这也是为什么我们在seek方法前执行poll方法
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

    /**
     * 整个KafkaConsumer大体有三个部分：
     * 1.设置拉取数据的位点（可选的）
     * 2.和broker建立连接，拉取数据
     * 3.提交位点
     */
    @Test
    public void testCommitSync() {
        Properties properties = new Properties();
        // consumer连接的初始服务器。因为consumer会被分配多个分区，每个分区可能存在于不同的broker中。这样consumer需要和broker集群中的多个broker都进行连接。
        // 我们在配置consumer时，仅需要填broker集群的一两个broker，consumer会向配置的broker发起集群嗅探的请求，broker接到请求后，会将集群中的所有broker连接方式都发送给consumer。这样consumer就可以向集群中所有broker发起连接请求了。
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // KafkaConsumer允许我们在程序中使用自己的类型（例如value的类型是BdWaybillInfo），但前提是需要给kafka提供将从broker拉取到的数据转换成我们需要的类型的转换器。
        // key.deserializer和value.deserializer分别用于提供对拉取的数据的key和value进行转换的处理类。
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        // kafka有两种订阅模式，一种是以加入到group中的subscribe模式，另一种是直接消费指定分区的assign模式。使用subscribe模式可以实现位点迁移、动态负载均衡（我们往相同组中加入新的consumer，会剥夺当前consumer分配的一些分区，移动给新的consumer，这样就减少了当前consumer的负载）
        // 当我们使用subscribe订阅模式时，需要提供一个groupId，consumer会像GroupCoordinator发起加入组的请求
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello-group");
        // 当前consumer客户端的一个命名，使我们在broker中可以看到都有哪些客户端获取到了分区
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-client");
        // 是否允许自动提交位点。如果启动的话，consumer会开启一个线程，定时的提交位点
        // 通常我们是需要手动提交位点的，因为一旦我们提交位点了，那么在broker中就记录了该group中，该分区已消费到了哪儿。下次再有consumer获取该分区后，会自动从该位置拉取数据。
        // 我们提交位点的前提应该是我这次拉取的数据都正确处理完成了，而自动提交是无法实现这个保证的，它就认为过了指定时间后，就可以提交位点了，它就把位点信息提交给broker。所以在自动提交位点情况下，有可能数据处理不正确，但位点依旧提交出去了。
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 当enable.auto.commit为true时，也就是自动提交位点时，指定每隔多长时间进行一次位点提交
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        // 当consumer和分配的分区的broker建立好连接后，它在拉取数据之前需要先向broker获取要拉取的位点信息。如果当前consumer的group在broker中没有位点提交信息，那么broker会根据该配置给consumer返回位点信息。
        // 如果该值为earliest，那么broker会把该分区第一个数据的offset发给consumer，代表consumer需要从该分区的第一条数据来拉取。如果该值为latest，那么broker会把该分区最后一个数据的offset发送给consumer，代表consumer需要从该分区最后一条数据来拉取。
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 每个分区最大拉取的数据数量，如果拉取一个分区中，第一条数据的大小就超过了该配置，那consumer就不会继续从该分区再拉取数据了
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(DataSize.ofKilobytes(3).toBytes()));
        // 本次poll请求最大拉取的数据量，也就是说从所有分配给这个consumer的分区中拉取到的所有数据的数据量的最大值
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(DataSize.ofKilobytes(20).toBytes()));
        // 本次poll请求最大拉取的数据条数。它和fetch.max.bytes是谁先到达就用谁的关系，例如fetch.max.bytes配置为100MB，但max.poll.records配置为10，那么consumer依然会只最多拉取10条数据，反之亦然。
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        // 一次poll请求最少拉取的数据量，如果broker中数据量少于该配置，那么broker不会马上对这个POLL REQUEST进行响应，而是暂时挂起当前请求，直到broker中收到足够的数据后才会给当前REQUEST进行响应。
        // 这样可以减少client向broker发起请求的频率，但会增加用于线程调用consumer的poll方法的阻塞时间
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(DataSize.ofBytes(1).toBytes()));
        // 由于fetch.min.bytes可能使当前poll request进行挂起等待，为了避免长时间让consumer进行等待，可以配置该参数，这样broker会有两个条件来对挂起的request进行响应：1.broker中有足够数量的数据了 2.broker判断挂起时长超过该配置了
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        // consumer在运作时，会向broker发起很多请求，例如JoinGroupRequest、LeaveGroupRequest，该配置用于控制请求发出去后需要最晚多长时间接收到响应。如果超过该配置没有收到响应，那么consumer会再次发起请求或把这次请求认定为请求失败。
        // 例如该配置为3秒，当consumer在10:50发起请求后，如果10：53还没有接收到broker的响应，那么consumer就会发起重试或认定该请求发送失败
        properties.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        // 配置consumer的拦截器，多个拦截器以逗号分割。consumer拦截器的主要作用是对consumer的poll、commit等方法获得broker的响应后进行的拦截
        properties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, BdWaybillInfoConsumerInterceptor.class.getName());
        // kafkaConsumer在启动后，会开启一个线程来监听consumer poll方法的调用频率，如果调用频率低于该配置，那么该线程会主动向GroupCoordinator发起LeaveGroupRequest，让broker从当前分组中把该client剔除掉，并且把分配给它的分区收回，分配给分组中其他active的consumer。
        // consumer两次调用poll()方法拉取数据，如果超过了该间隔，当前consumer会给broker发送一个leave group request，这样broker就会把当前consumer剔除出group，将分配给它的分区分配给组内其他正在保持心跳的consumer
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(26665).toMillis()));
        // 当consumer加入一个group成功并获取到分区后，它就会和broker建立连接。之后的一个问题broker如何判断这个consumer是否一直存活。kafka使用consumer定时向GroupCoordinator发送心跳来解决这个问题。
        // 每隔heartbeat.interval.ms时间，consumer就会定时给GroupCoordinator发送一个心跳，证明自己还存活。但是如果有一次心跳超时就判定该consumer不存活了，又过于严格。
        // 所以就有了session.timeout.ms配置，在session.timeout.ms配置的范围内，如果没有consumer没有发送过一个心跳包，GroupCoordinator就可以认为该consumer不存活了。
        // 一般情况，heartbeat.interval.ms要小于session.timeout.ms的1/3大小。
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofMillis(800).toMillis()));
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofMillis(50000).toMillis()));

        /*
         * KafkaConsumer在底层的大部分操作，都是交由ConsumerCoordinator来完成，而ConsumerCoordinator大部分和kafka broker交互的动作，都是consumer找到GroupCoordinator并提交给GroupCoordinator来完成。
         * 例如ConsumerCoordinator的commit操作，就是将位点信息发送给GroupCoordinator，由GroupCoordinator来往__commit_offsets中存储提交进来的位点信息。
         * 注意ConsumerCoordinator和GroupCoordinator的区别，它们虽然都是Coordinator，但是ConsumerCoordinator是执行在client端，它是KafkaConsumer具体处理的实现。而GroupCoordinator则其实是broker集群中的一个node
         */
        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        /*
         * 当调用poll方法时，参考ConsumerCoordinator的poll方法：
         * 1.如果当前consumer还没有和broker建立连接，则发起建立连接流程
         *   a) 向最小负载的broker节点上发送FindCoordinatorRequest，获取GroupCoordinator
         *   b) 向GroupCoordinator发送JoinGroupRequest，JoinGroupRequest中带着当前consumer的分区分配策略
         *   c) GroupCoordinator从该group的consumer中选出一个，作为leader consumer，其余为follower consumer
         *   d) GroupCoordinator收集到这个group的所有consumer的分区分配策略后，从中选出被所有consumer支持最多的分区分配策略，作为实际分区分配结果。选出后给leader consumer的响应中有分区分配策略，而follower consumer收到的响应中没有分区分配策略
         *   e) leader consumer在收到分区分配策略后，根据策略进行分区分配，生成分配的具体方案，即给每个consumer分配哪些分区
         *   f) 在收到GroupCoordinator对JoinGroupRequest的正确响应后，每一个consumer都要向GroupCoordinator发起SyncGroupRequest。leader consumer的SyncGroupRequest请求中包含了各个consumer的分区分配的具体方案，follower consumer中则没有
         *   g) GroupCoordinator将分区分配的具体方案作为SyncGroupRequest的响应，发送给每一个consumer。此时该consumer就可以正常工作了，在次之前它会开启一个心跳线程，定时向GroupCoordinator发送心跳，以表明当前consumer是存活的。
         *   h) 在consumer拉取数据之前，它需要知道这个分区的拉取位点。它会向GroupCoordinator发起OffsetFetchRequest，GroupCoordinator收到请求后，会从__commit_offset中找到这个group中该partition对应的位点信息，发送给consumer
         *   i) 如果启动了自动提交位点（enable.auto.commit=true），且距离上次提交位点已经过了auto.commit.interval.ms指定的距离，就会发起一次异步位点提交请求
         * 上面这些过程其实就是consumer从不知道分配的分区到获取到broker分配给它的分区的过程
         * 2.经过上述步骤后，consumer已经知道了要拉取的分区以及要拉取分区的位点信息，那么它就要开始拉取数据了。它就不向GroupCoordinator发送消息了，而是向各个分区的所在broker发起FetchRequest，开始进行数据拉取了
         * 注意：
         * 1.在consumer获取到分区之前，都是ConsumerCoordinator对GroupCoordinator发起请求，而consumer获取到分配的分区以后，就是直接向分区对应的broker发起请求了。
         * 2.下一次调用poll方法时，如果当前consumer已经在group中了，就不用再经过步骤1了，直接向分配的分区对应的broker拉取数据即可。
         * 3.自动提交位点机制是通过不断调用KafkaConsumer的poll方法来实现的，通过ConsumerCoordinator的poll方法触发位点提交动作
         *
         * 当consumer和broker建立好连接后，consumer会开启一个线程（心跳线程），心跳线程主要用于：
         * 1.定时向GroupCoordinator发送心跳，报告当前consumer处于存活状态
         * 2.监控距离上一次poll的时间间隔，当超时后，会向GroupCoordinator发送LeaveGroupRequest
         * 3.监控在session.timeout.ms配置的时间范围内，是否有发送过心跳包，没有则向GroupCoordinator发送LeaveGroupRequest
         *
         * 当consumer和broker建立好连接开始拉取数据后，有以下情况会使consumer给GroupCoordinator发起LeaveGroupRequest
         * 1.consumer在session.timeout.ms配置的时间范围内，没有发过一次心跳包
         * 2.consumer在最后一次调用poll方法后，超过max.poll.interval.ms毫秒都没有进行下一次poll方法的调用
         * 3.consumer主动调用unsubscribe方法，对topic进行取消订阅
         */
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
                    提交位点的流程，参见ConsumerCoordinator的commitOffsetsSync方法：
                    1.向负载最小的broker节点发起FindCoordinatorRequest，获取GroupCoordinator
                    2.向GroupCoordinator发起OffsetCommitRequest请求，请求中附带了每个分区的位点信息
                    3.GroupCoordinator在收到OffsetCommitRequest请求后，将该group的分区的位点信息提交到__commit_offset中存储
                    所以：我们提交位点，本质上就是要把位点信息写入到__commit_offset中，让broker知道，该group中对于该分区已经处理完指定offset之前的数据了，下次拉取数据从该位置拉取数据即可

                    1.为什么要手动提交位点，因为自动提交位点的话，在拉取数据指定时间之后，consumer就认为数据处理正确了，可以提交位点了，但真正这批数据拉取完以后处理的正不正确，只有用户程序知道。所以自动提交位点有可能产生的问题是位点提交了，
                      但是这批拉取的数据处理异常，但是由于位点已经提交，无法再次拉取到处理错误的数据了（除非consumer主动使用seek）。所以我们在处理位点时，还是应尽量使用手动提交，在拉取的数据正确处理后再提交位点。
                    2.每次拉取并处理完数据时就进行一次commit，为什么要这样做？假如多次拉取，最后一起commit，有可能造成的问题是假设前两次拉取数据和处理数据没问题，而第三次拉取和处理数据出现异常了，导致位点提交没有执行，这样这三次拉取的数据都白处理了
                      ，下次再拉取数据还是得从第一次拉取的位置拉取数据（因为没有提交位点），这就导致了数据的重复处理
                    3.每次拉取并处理完数据时进行一次commit，那么仅有可能在出现异常时，让这次拉取的数据白处理了，也就是只重复处理了这次拉取的数据。
                    4.位点提交也要注意提交的频率，因为每一次提交都需要和GroupCoordinator进行通信，并且是阻塞的（因为是同步提交），在kafka没有对提交位点操作给出回应之前，用户线程都处于阻塞状态。
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

    @Test
    public void testAssignWithGroup() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tx-group5");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testCli5");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(DataSize.ofKilobytes(20).toBytes()));

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor("hello_tx");
            /*
             * 在assign时也可以使用groupId来获取partition在指定groupId对应的位点的数据，并且为指定groupId进行位点提交。
             * assign的坏处是无法进行动态横向扩展，当group有新consumer时，也无法为当前consumer减少一些partition。
             * 但assign的好处是，可以交由程序进行分区分配，因为在subscribe中，每个consumer被分配的partition是由consumer的分区分配策略决定的，而使用assign，可以让用户手动指定每个consumer分配哪个partition
             * 下面的程序就是模拟我们通过程序手动为当前KafkaConsumer分配一个partition，而不是由分区分配策略决定
             */
            PartitionInfo assignPartition = partitionInfoList.get(0);
            kafkaConsumer.assign(Collections.singleton(new TopicPartition(assignPartition.topic(), assignPartition.partition())));
//            kafkaConsumer.assign(partitionInfoList.stream().map(pi->new TopicPartition(pi.topic(),pi.partition())).collect(Collectors.toList()));
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2));
            kafkaConsumer.commitSync();
        }
    }

    @Test
    public void testCommitWithoutPoll() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tx-group5");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // kafka允许consumer在不订阅topic和不获取partition的情况下，也可以提交提交位点。也是为properties中group.id中的group来提交位点
        // 由于kafka允许这样处理，因此可以将topic订阅、拉取与位点提交放在两个不同的consumer来处理，只要他们的properties中group.id是相同的就可以，一个只负责拉取数据，另一个只负责提交位点（这个确实玩儿的挺花，也是看flink kafka source发现的）
        // 注意：但是拉取数据时，consumer必须要通过subscribe或者assign订阅topic并且获取到partition，才能够拉取到数据
        kafkaConsumer.commitSync(Collections.singletonMap(new TopicPartition("hello_tx",0),new OffsetAndMetadata(10)));
        kafkaConsumer.close();
    }
}
