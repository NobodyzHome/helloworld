package com.mzq.hello.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mzq.hello.domain.BdWaybillOrder;
import com.mzq.hello.domain.WaybillC;
import com.mzq.hello.util.GenerateDomainUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.springframework.util.unit.DataSize;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * kafka基本概念
 * 一个分区可以有多个副本，其中只有一个是leader副本，其他都是follower副本。同时，kafka要求在同一个broker中，只能存放一个partition的一个副本（可以是leader副本，也可以是follower副本），因此leader副本和follower副本肯定不在一个broker中。
 * 客户端（不论是生产还是消费）只对leader分区进行写入和读取，follower副本只负责从leader副本中同步数据。
 * 生产客户端只会将数据发送到对应分区的leader副本，其他broker会定时将leader副本中的数据同步到follower副本中。由于网络等因素，多个broker将leader副本的数据同步到follower副本的效率也是不同的，因此产生了以下概念：
 * 【ISR】:leader副本和那些与leader副本中的数据差不多follower副本（同步效率比较好的），我们称之为ISR（in sync replicas）。
 * 【OSR】:与ISR对应的，那些与leader副本中数据查很多的follower副本，我们称之为OSR(out of sync replicas)而我们将一个分区的全部副本。
 * 【AR】:leader副本以及所有的follower副本组成了AR（assigned replicas）
 * 【AR】 = 【ISR】 + 【OSR】
 * <p>
 * leader副本负责跟踪和维护ISR，如果一个ISR中的follower突然与leader差了很多数据，那么leader副本会将它踢出ISR中；如果一个OSR中的follower突然与leader的数据追平了，那么leader副本会将它加入到ISR。
 * 当leader副本宕机后，kafka会从ISR中选出新的leader副本，而OSR中的follower则没有机会。
 * <p>
 * ISR还决定了HW（high watermark），而HW决定了消费者可以消费的数据内容。例如ISR中有三个副本（1个leader和2个follower），当producer向leader中新插入了两条数据后，那么leader的LEO(LOG END OFFSET，即副本中最后一个offset值+1)为6，
 * 随后follower从leader中拉取数据，假设某一时刻，其中一个follower把这两条数据都拉取到了（LEO=6），另一个follower只拉取到一条数据（LEO=5），那么这个分区的HW取leader和follower中LEO最小值，也就是5，因此HW=5，
 * 代表了消费者对于这个分区只能消费leader副本中offset=0~4的数据。而随后当另一个follower也把leader中的数据拉全了（即LEO从5变成6），那么这个分区的HW则变为6，代表消费者可以消费leader副本中offset=0～5的数据。
 * 因此：虽然客户端只是从leader副本拉数，但leader副本和ISR中follower副本的同步情况决定了客户端能从leader副本中拉取哪些数据，也就是说往leader副本中新插入一条数据，这条数据是不能被立即消费的，而是需要和ISR中的所有follower同步完毕才能被消费。
 */
@Slf4j
public class ProducerTest {

    /**
     * 用户线程send方法负责：
     * 1.将ProducerRecord依次交由拦截器、序列化器、分区器处理
     * 2.将ProducerRecord转换成ProducerBatch，然后添加到RecordAccumulator
     * <p>
     * Sender线程负责：
     * 1.从RecordAccumulator中拉取ProducerBatch，然后转换成Request，发送到partition对应的broker节点
     * 2.监听kafka broker对Request给出的响应，根据响应结果清理RecordAccumulator中已响应的ProducerBatch
     * 3.根据broker响应中返回的异常，将可重试异常的ProducerBatch重新加入到RecordAccumulator中
     * 注意：
     * 1.用户线程起始于调用KafkaProducer的send方法，结束于send方法内部将ProducerBatch存入RecordAccumulator，返回的Future是将来sender线程将ProducerBatch发送到broker后，broker对发送结果的响应。(类似hbase的put方法，只需要把数据写入到memstore，即完成了写入流程)
     * 2.sender线程起始于从RecordAccumulator中拉取ProducerBatch，结束于接收到broker的响应并清理已处理完的RecordAccumulator的ProducerBatch，并把用户线程send方法返回的Future置为处理完成
     * 3.无论ProducerRecord是在RecordAccumulator还是在in-flight request中，数据还是在producer客户端中，没有被broker记录。只有request被发送到broker并且broker成功响应后，数据才真正存储到了broker中，才可以被消费者消费。
     */
    @Test
    public void basicUsage() throws ExecutionException, InterruptedException, TimeoutException {
        Properties properties = new Properties();
        /*
         * producer在写入数据时，根据分区器判定要写入的分区，会写入到不同的broker中。但在填写bootstrap.server时，只需要给出broker集群中一两个broker的连接地址即可。
         * 这是因为当客户端连接到一个broker后，会给broker发送ClusterMetaRequest，broker会访问zookeeper下面的/broker/ids目录，获取broker集群中的每一个broker信息，然后返回给客户端。
         * 这样客户端就知道broker集群中所有broker的连接信息了。
         */
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "producer-client");
        // max.request.size用于控制ProducerRecord的最大大小（单位是byte），当ProducerRecord经过序列化器序列化后，key和value的大小的和，再加上一些请求需要的信息，构成了该请求的大小。如果该大小超过了配置，就会报RecordTooLargeException异常，这个请求都不会写入到RecordAccumulator中。
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(5).toBytes()));
        // RecordAccumulator中存储的每一个ProducerBatch的最大容量。当发来一个ProducerRecord时，如果它的大小比小batch.size小，那么会尝试把它放到已有的ProducerBatch中。
        // 否则会创建一个ProducerBatch，它的大小和ProducerRecord的大小一样，但注意这种超过batch.size的ProducerBatch不会放到对象池中进行对象复用
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(10).toBytes()));
        // send、partitionsFor等方法阻塞用户线程（因为它们的执行是在用户线程中执行的）的最大时间，当这些方法执行时间超过该配置，用户线程则会收到TimeoutException异常，以此来解除用户线程的阻塞
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Duration.ofSeconds(3).toMillis()));
        // RecordAccumulator中存储待发送数据的最大内存。如果RecordAccumulator中存储的ProducerBatch容量已超过该配置，当新插入一条ProducerRecord时，那么当前线程会阻塞，等待RecordAccumulator中有请求被响应，从而从RecordAccumulator中提出，腾出足够的空余的容量
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DataSize.ofMegabytes(2).toBytes()));
        /*
         * 该参数是发送给broker，broker收到该参数后，会根据参数的值决定【何时】对发来的请求进行响应：
         * 1.ack=0，broker在收到请求后，就立即对请求给出响应。这种情况下，send方法返回的RecordMetadata中，offset就固定为-1，因为响应结果是不等数据写入到副本后就发送了。
         * 2.ack=1，broker在收到请求后，会将数据写入到leader副本，然后就对请求给出响应。因此在此时，send方法返回的RecordMetadata中，offset就是实际的数据写入到副本中的offset。
         * 3.ack=all或-1，broker在收到该请求后，会将数据写入到leader副本，然后等待ISR中的其他broker同步完该条数据，才会给出响应。因此send方法返回的RecordMetadata中也会带有数据写入到副本的实际的offset。
         *
         * 注意：
         * 1.无论ack等于几，producer都是异步发送record，即producer只把record加入到内存中的RecordAccumulator就结束了，用户线程就不再阻塞了。而ack影响的是send方法返回的Future对象被响应的速度，该Future对象只有在broker对其发送响应后才会被赋值。
         * 2.如果ack=0，那么retries就无效了，因为客户端并没有等到broker对请求的响应结果，因此也就不知道响应结果中是否有异常，导致无法判断是否需要重试
         */
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        // 当请求发送到broker后，如果处理请求发生错误（例如网络闪断等），那么broker给客户端的响应中包含exception属性。Sender线程在收到该响应后，会根据该属性的判断是否需要对该ProducerBatch进行重试。如果该属性大于0，则会将该ProducerBatch重新写入到RecordAccumulator，用于重新发送该请求。
        // 注意：不是所有响应的异常都可以重试，继承自RetriableException下面的异常才是可重试的异常。例如RecordTooLargeException异常则是不能重试的。
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        // 设置Sender线程在收到kafka响应的异常多少毫秒后，才发起重试，避免在短时间内进行多次重试
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        /*
         * Sender线程在将请求发送到broker后，超过request.timeout.ms配置的时间后，如果broker还没有给出响应，则认为请求超时，此时可能对该请求进行重试发送或认定请求发送超时异常。
         * 注意：这里发送的请求不仅是写入数据的请求，也可以获取集群信息的请求、获取topic的分区信息等请求。总之是所有请求在发出去以后的超时时间。
         */
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "50");
        /*
         * Sender线程在从RecordAccumulator中拉取到ProducerBatch后，在将ProducerBatch发送到broker前，会判断当前ProducerBatch是否过期了，就是使用【当前时间 - ProducerBatch的创建时间 > delivery.timeout.ms】来判断是否过期。
         * 如果有过期，则Sender线程将对应的ProducerBatch的处理结果ProduceRequestResult的exception属性赋值TimeoutException。
         * 此时主线程调用send方法返回的FutureRecordMetadata的get方法时，get方法内发现ProduceRequestResult的exception属性被赋值了，则会抛出异常(异常内容：Expiring 1 record(s) for hello-world-0:11 ms has passed since batch creation)。
         *
         * 发生该异常主要原因是：KafkaProducer.send()的频率大于Sender线程从RecordAccumulator中拉取ProducerBatch并发送至broker然后清理已响应的ProducerBatch的速度，导致ProducerBatch在RecordAccumulator中驻留的时间超过了delivery.timeout.ms的配置。
         * 解决方案：1.我们可以提高该配置，降低ProducerBatch过期几率 2.增加该topic对应的分区数，使send()方法发来的ProducerBatch存储于不同的分区中
         * 注意：此时超时的ProducerBatch是无法通过retries参数进行重试的，因为retries控制的是broker给出响应后，发现响应中有可重试的异常才会重试。但这种情况下，Sender线程根本没有往broker里发送ProducerBatch，所以无法重试。
         */
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1000");
        /*
         * 当我们发送消息时，为了减少网络I/O的使用，producer在发送的ProducerRecord时，会把数据先存储到内存中，具体是存储到ProducerBatch中。此后，producer会在以下情况将内存中的ProducerBatch发送给broker：
         * 1.ProducerBatch的大小超过batch.size，代表该ProducerBatch已经装满数据了，可以被发送出去了
         * 2.ProducerBatch的大小虽然小于batch.size，但在RecordAccumulator中存在的时间也超过了linger.ms时间。
         * linger.ms配置的目的是：让ProducerBatch即使没有满足大小的要求，也可以及时发送到broker中，不会因为一直不生产新的ProducerRecord，导致数据都积压在producer的内存中
         *
         * 注意：一个ProducerBatch如果在RecordAccumulator中存在时间超过delivery.timeout.ms后也没发送到broker中，那么KafkaProducer会抛出异常。因此linger.ms应该远小于delivery.timeout.ms，
         * 让producer尽量不会因为ProducerBatch存储在内存中时间过长而抛出异常。
         */
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "600");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        /*
         * ProducerRecord是要发送到kafka broker中的数据，在ProducerRecord中，最少要提供topic和value两个属性。
         * 1.topic（必填）：要将数据发送的topic
         * 2.value（必填）：发送数据的主要内容
         * 3.key（选填）：对数据的概括，也是默认分区器用于判断这条数据应该发送到哪个分区的重要依据。相同的key会被发送到同一个分区中
         * 4.partition（选填）：我们可以在ProducerRecord中指定数据要发送的分区，这样数据就不用经过分区器来判断分区了
         * 5.timestamp（选填）：代表这条数据产生的时间，如果没有设置的话，默认使用System.currentTimeMillis()
         *
         * 注意：用户在使用KafkaProducer时，只需要给出topic即可，不需要知道这个topic的详细信息（例如有多少个分区、每个分区的副本都在哪些broker上）。在send方法内会向kafka broker发起请求，获取集群信息。
         */
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("hello-world", "zhangsan", "test-lisi");
        /*
         * send方法执行内容：注意，这些内容都是在当前用户线程执行的，是串行执行的
         * 1.调用拦截器的onSend方法
         * 2.向kafka server发送metaDataRequest，获取broker集群信息以及topic的元数据（例如有多少个分区）
         * 3.将ProducerRecord的key和value经由serializer进行序列化
         * 4.将数据经由Partitioner，获取需要发送的分区
         * 5.将数据写入到RecordAccumulator中对应分区的ProducerBatch中，这步中有可能因为RecordAccumulator中占用的内存量超过配置的最大内存量（buffer.memory）而阻塞用户线程，
         *   直到RecordAccumulator剔除了得到响应的ProducerBatch，RecordAccumulator中拥有了足够的容量后会恢复当前当前线程的执行
         * 6.从上述说明可以看到，send方法在执行时是在用户线程串行执行的。我们可以通过设置max.block.ms参数，来决定send方法占用当前线程的最大时间。如果send方法执行时间超过该时间，当前线程则会收到异常，从而结束send方法带来的当前线程的阻塞。
         * 7.send方法返回的是一个future，这个其实容易非常让人产生误区，感觉好像send方法是异步执行的，返回给用户线程的是send方法的执行结果。但其实不是，send方法是在用户线程串行执行的，而send方法返回的这个Future，是sender线程把这个请求
         *   发送到服务器端进行响应的结果。在收到服务器的响应结果前，调用Future.get会被阻塞，而收到响应后，则可以获取到服务器的响应结果。其中主要包括这条数据存储于哪个partition中、offset和timestamp是什么等。
         */
        Future<RecordMetadata> recordMetaDataResultFuture = kafkaProducer.send(producerRecord);
        Future<RecordMetadata> recordMetaDataResultFuture1 = kafkaProducer.send(producerRecord);
        // recordMetaDataResultFuture返回的是broker对该请求所存储到的ProducerBatch的响应结果，在服务器对这个ProducerBatch给出响应前，调用get方法会被阻塞。Sender线程在收到broker响应后，会把该Future设置为done，这样用户线程就可以解除阻塞，获取服务器的响应数据了。
        RecordMetadata recordMetadata = recordMetaDataResultFuture.get();
        // 由于一个ProducerBatch中可以存储多个ProducerRecord，因此recordMetadata1和recordMetadata这两个Future等待的都是同一个ProducerBatch的响应结果
        RecordMetadata recordMetadata1 = recordMetaDataResultFuture1.get();
        // 主线程获取服务器的响应结果并使用
        System.out.println(recordMetadata);

        // KafkaProducer用完后一定要记得关闭，断开和broker的连接
        kafkaProducer.close();
    }

    /**
     * 一般来说，异步处理有两种方式：
     * 1.一是给主线程返回一个Future，由主线程主动来推测Future是否完成，完成时调用get方法会解除阻塞
     * 2.二是主线程在调用异步处理的方法时，传入一个callback，当子线程执行完毕后，会新起一个线程来执行callback的内容
     * <p>
     * 第一种情况适用于主线程需要获取子线程的执行结果才能继续往下走的情况
     * 第二种情况适用于主线程不关心子线程的执行结果的情况
     * <p>
     * 我们现在说的这个就是第二种情况的使用方法
     */
    @Test
    public void testSendWithCallback() throws ExecutionException, InterruptedException {
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BdWaybillInfoDeSerializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello-group");
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "hello-client");
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        String topic = "bd-waybill-info";
        KafkaConsumer<String, BdWaybillOrder> kafkaConsumer = new KafkaConsumer<>(consumerConfig);
        kafkaConsumer.subscribe(Collections.singleton(topic));

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(20).toBytes()));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(2).toBytes()));
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DataSize.ofKilobytes(30).toBytes()));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(1).toMillis()));
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));

        try (KafkaProducer<String, BdWaybillOrder> kafkaProducer = new KafkaProducer<>(properties)) {
            List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(20);
            for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
                ProducerRecord<String, BdWaybillOrder> producerRecord = new ProducerRecord<>(topic, bdWaybillOrder.getWaybillCode(), bdWaybillOrder);
                 /*
                    sender线程在收到服务器响应后，会调用callback，省去了我们自己在用户线程判断Future是否已执行完毕的过程
                    但这种方式也有个问题就是：callback是在sender线程执行的，我们知道sender线程用来和broker交互，如果我们的callback执行时间过长，会耽误sender来执行它本身的工作，后果会很严重。
                    所以：要么不使用这种方式，如果必须使用，则callback的逻辑必须要很轻
                 */
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        log.info("producer callback!key={},partition={},timestamp={}", bdWaybillOrder.getWaybillCode(), metadata.partition(), metadata.timestamp());
                    }
                });
            }
        }

        ConsumerRecords<String, BdWaybillOrder> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(500));
        for (TopicPartition topicPartition : consumerRecords.partitions()) {
            List<ConsumerRecord<String, BdWaybillOrder>> partitionRecords = consumerRecords.records(topicPartition);
            for (ConsumerRecord<String, BdWaybillOrder> consumerRecord : partitionRecords) {
                log.info("consumer record!key={},value={},topic={},partition={},timestamp={}", consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), consumerRecord.partition(), consumerRecord.timestamp());
            }
        }
        kafkaConsumer.close();
    }

    @Slf4j
    public static class ChangeKeyProducerInterceptor implements ProducerInterceptor<String, BdWaybillOrder> {

        private ObjectMapper objectMapper;

        /**
         * 该方法是在用户线程中执行，当调用KafkaProducer.send()方法时，首先会把请求交由该方法来处理，该方法返回的ProducerRecord才会经过后续序列化器、分区器的处理。
         * 当然我们非常不赞成在拦截器中修改ProducerRecord的内容，这样的做法太隐晦了，出了问题很难排查。
         */
        @Override
        public ProducerRecord<String, BdWaybillOrder> onSend(ProducerRecord<String, BdWaybillOrder> record) {
            // 这个例子中，我们借助了拦截器来修改发送的数据的key，从而让同一个运单号的数据去到不同的分区
            // 但这只是演示，实际生产上还是不建议用拦截器来修改数据
            String newKey = String.format("%s-%d", record.key(), RandomUtils.nextInt(100, 1000));
            log.info("收到发送的数据，已将key进行修改，用于将相同运单的数据打散到不同分区中。原始key={}，修改后的key={}", record.key(), newKey);
            return new ProducerRecord<>(record.topic(), newKey, record.value());
        }

        /**
         * 该方法是在Sender线程中执行，当broker收到对应ProducerBatch的响应后，会调用该方法。
         * 注意：同KafkaProducer.send(ProducerRecord,Callback)一样，该方法也是占用了Sender线程的处理内容，所以该方法的实现应该是非常轻的
         * This method will generally execute in the background I/O thread, so the implementation should be reasonably fast.
         */
        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if (Objects.nonNull(exception)) {
                log.error("发送数据失败！", exception);
            } else {
                log.info("发送数据成功！topic={},partition={},offset={},timestamp={}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            }
        }

        /**
         * 该方法是在调用KafkaProducer.close时调用的，用于让拦截器做一些结束工作的处理（例如释放内存等）
         */
        @Override
        public void close() {
            // 将初始化时生成的ObjectMapper设置为null，减少内存的使用
            objectMapper = null;
            log.info("拦截器关闭完毕");
        }

        /**
         * 在创建KafkaProducer时会执行该方法，用于初始化当前拦截器。其中入参configs是创建KafkaProducer时用户传入的参数，由于它的范型是Map<String, ?>，因此只能获取属性值，没办法设置属性值
         * 我们也可以利用该方法作为初始化的方法，进行一些初始化的工作。
         */
        @Override
        public void configure(Map<String, ?> configs) {
            // 获取初始化参数中的client.id属性
            String clientId = (String) configs.get(ProducerConfig.CLIENT_ID_CONFIG);
            // 进行当前监听器的初始化工作，在这里就是创建一个ObjectMapper
            objectMapper = new ObjectMapper().disable(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS);
            //
            String contents = null;
            try {
                contents = objectMapper.writeValueAsString(configs);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            log.info("拦截器初始化完毕。clientId={},初始化参数：{}", clientId, contents);
        }
    }

    @Test
    public void testInterceptor() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "my-client");
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(20).toBytes()));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(50).toBytes()));
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DataSize.ofMegabytes(5).toBytes()));
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(Duration.ofSeconds(5).toMillis()));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofMillis(500).toMillis()));
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        // 拦截器类列表，多个拦截器之间使用逗号分割
        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ChangeKeyProducerInterceptor.class.getName());

        KafkaProducer<String, BdWaybillOrder> kafkaProducer = new KafkaProducer<>(properties);
        List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(20);
        String waybillCode = bdWaybillOrders.get(0).getWaybillCode();
        bdWaybillOrders.forEach(order -> order.setWaybillCode(waybillCode));

        for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
            ProducerRecord<String, BdWaybillOrder> producerRecord = new ProducerRecord<>("bd-waybill-info", bdWaybillOrder.getWaybillCode(), bdWaybillOrder);
            // 由于我们设置了拦截器，所以我们在这里可以不等待send方法返回的Future（也就是不等待broker对该ProducerBatch的响应结果）
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                log.info("waybillCode={},partition={},offset={}", bdWaybillOrder.getWaybillCode(), metadata.partition(), metadata.offset());
            });
        }

        kafkaProducer.close();
    }

    @Slf4j
    public static class WaybillcInterceptor implements ProducerInterceptor<String, WaybillC> {
        private ObjectMapper objectMapper;

        @SneakyThrows
        @Override
        public ProducerRecord<String, WaybillC> onSend(ProducerRecord<String, WaybillC> record) {
            String value = objectMapper.writeValueAsString(record.value());
            log.info("即将发送数据：key={},value={}", record.key(), value);
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if (Objects.nonNull(exception)) {
                log.error("发送数据失败！", exception);
            } else {
                log.info("发送数据成功！partition={},offset={}", metadata.partition(), metadata.offset());
            }
        }

        @Override
        public void close() {
            objectMapper = null;
            log.info("拦截器关闭完毕。");
        }

        @Override
        public void configure(Map<String, ?> configs) {
            objectMapper = new ObjectMapper();
            log.info("拦截器初始化完毕。启动参数：{}", configs);
        }
    }

    public static class DeepCopySerializer<T extends Serializable> implements Serializer<T> {

        @Override
        public byte[] serialize(String topic, T data) {
            return SerializationUtils.serialize(data);
        }
    }

    public static class DeepCopyDeserializer<T extends Serializable> implements Deserializer<T> {

        @Override
        public T deserialize(String topic, byte[] data) {
            return SerializationUtils.deserialize(data);
        }
    }

    @Test
    public void testSerializer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        /*
         * 我们知道，客户端在向broker发送数据时，使用的肯定是二进制字节码。但在程序中，我们的数据是java的各种数据类型。这里就产生一个问题：如何将程序中的各种数据类型转换成二进制字节码。
         * KafkaProducer允许我们发送数据的key和value是程序中的数据类型，由KafkaProducer内部来进行二进制字节码的转换，但这前提就是要在KafkaProducer中给出key和value的
         * org.apache.kafka.common.serialization.Serializer。KafkaProducer会使用他们来将发来的ProducerRecord中的key和value转换成二进制字节码。
         * 也就是：程序把数据发出来 -> KafkaProducer把数据的key和value转换成二进制 -> KafkaProducer把二进制数据发送给broker
         *
         * 在KafkaProducer的范型中，就是Key和Value的类型，当我们给KafkaProducer的范型的范型赋值后，就要求发送的ProducerRecord中key和value是对应类型的数据。
         * 因此我们就要设置对应的key和value类型的Serializer，让KafkaProducer可以对key和value这两种类型的对象进行二进制字节码转换。
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(5).toBytes()));
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DataSize.ofMegabytes(10).toBytes()));
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(2).toBytes()));
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Duration.ofSeconds(5).toMillis()));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(Duration.ofSeconds(5).toMillis()));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(3).toMillis()));
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(20).toMillis()));
        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, WaybillcInterceptor.class.getName());
        // 当前partitioner使用的是kafka提供的另一个Partitioner的实现，它不再是按照key来分区，而是随机分配，让每个分区的数据量大致一样
//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        try (KafkaProducer<String, WaybillC> kafkaProducer = new KafkaProducer<>(properties)) {
            List<WaybillC> waybillCList = GenerateDomainUtils.generateWaybillc(10);
            for (WaybillC waybillC : waybillCList) {
                ProducerRecord<String, WaybillC> producerRecord = new ProducerRecord<>("waybill-c", waybillC.getWaybillCode(), waybillC);
                kafkaProducer.send(producerRecord);
            }
        }

//        Properties consumerConfig = new Properties();
//        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeSerializer.class.getName());
//        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
//        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-client");
//        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        consumerConfig.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(Duration.ofSeconds(1).toMillis()));
//
//        try (KafkaConsumer<String, WaybillC> kafkaConsumer = new KafkaConsumer<>(consumerConfig)) {
//            kafkaConsumer.subscribe(Collections.singleton("waybill-c"));
//            ConsumerRecords<String, WaybillC> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2));
//            Set<TopicPartition> partitions = consumerRecords.partitions();
//            for (TopicPartition topicPartition : partitions) {
//                List<ConsumerRecord<String, WaybillC>> partitionRecords = consumerRecords.records(topicPartition);
//                log.info(StringUtils.center("====partition:" + topicPartition.partition() + "====", 50));
//                partitionRecords.forEach(consumerRecord -> log.info("consumer record.topic={},partition={},offset={},key={},value={}"
//                        , consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value()));
//            }
//        }

    }

    @Slf4j
    public static class RandomKeyPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            if (Objects.nonNull(key)) {
                List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
                int partitionCount = partitionInfos.size();
                String newKey = String.format("%s-%d", key, RandomUtils.nextInt(100, 1000));
                int newKeyHash = Math.abs(newKey.hashCode());
                int index = newKeyHash % partitionCount;
                return partitionInfos.get(index).partition();
            } else {
                List<PartitionInfo> availablePartitionsForTopic = cluster.availablePartitionsForTopic(topic);
                int partitionCount = availablePartitionsForTopic.size();
                int index = RandomUtils.nextInt(0, partitionCount);
                return availablePartitionsForTopic.get(index).partition();
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
    public void testPartitioner() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "my-client");
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(20).toBytes()));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(50).toBytes()));
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DataSize.ofMegabytes(5).toBytes()));
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Duration.ofSeconds(2).toMillis()));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(Duration.ofSeconds(1).toMillis()));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(5).toMillis()));
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(10).toMillis()));
        /*
         * partitioner.class用来决定一条ProducerRecord数据应发送到哪个分区，如果不设置，默认使用的是org.apache.kafka.clients.producer.internals.DefaultPartitioner
         * 它的处理策略如下：
         * 如果用户指定了要发往的分区，则使用该分区。If a partition is specified in the record, use it
         * If no partition is specified but a key is present choose a partition based on a hash of the key
         * If no partition or key is present choose the sticky partition that changes when the batch is full
         * 关于它的处理策略的补充说明：
         * 1.对于有key的ProducerRecord，它可以保证拥有相同key的数据都存储在一个分区里，我们知道，kafka可以保证在一个分区里的数据的顺序
         * 2.对于没有key也没有在ProducerRecord中指定分区的，它的策略是使用sticky partition，大概意思就是如果可用分区（cluster.availablePartitionsForTopic(topic)）>0，则从可用分区中随机选择一个，否则从全部分区（cluster.partitionsForTopic(topic)）中随机选择一个
         *
         * 在这里我们使用的是我们自己设置的分区器，它的作用是让相同key的数据也分配到不同的分区中
         */
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RandomKeyPartitioner.class.getName());

        try (KafkaProducer<String, BdWaybillOrder> kafkaProducer = new KafkaProducer<>(properties)) {
            List<BdWaybillOrder> bdWaybillOrders = GenerateDomainUtils.generateBdWaybillOrders(30);
            String waybillCode = bdWaybillOrders.get(0).getWaybillCode();
            bdWaybillOrders.forEach(order -> order.setWaybillCode(waybillCode));

            for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders) {
                ProducerRecord<String, BdWaybillOrder> producerRecord = new ProducerRecord<>("bd-waybill-info", bdWaybillOrder.getWaybillCode(), bdWaybillOrder);
                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    log.info("with key.waybillCode={},partition={},offset={}", bdWaybillOrder.getWaybillCode(), metadata.partition(), metadata.offset());
                });
            }

            List<BdWaybillOrder> bdWaybillOrders1 = GenerateDomainUtils.generateBdWaybillOrders(20);
            for (BdWaybillOrder bdWaybillOrder : bdWaybillOrders1) {
                ProducerRecord<String, BdWaybillOrder> producerRecord = new ProducerRecord<>("bd-waybill-info", null, bdWaybillOrder);
                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    log.info("without key.waybillCode={},partition={},offset={}", bdWaybillOrder.getWaybillCode(), metadata.partition(), metadata.offset());
                });
            }
        }
    }

    /**
     * 幂等性主要解决的问题是数据重复存储。假设有一个record从producer发送到broker，broker收到后进行落盘，在落完盘后，给producer发送响应前发生了异常。
     * 此时broker会给producer响应异常信息，producer此时如果开启的retry后，会再次将该record发送到broker，此时broker落完盘后，可以发现该record在broker中重复落盘了两次。
     * 幂等性就是为了解决该问题，针对已落盘而又重复发过来的数据，broker会抛弃，而不是继续落盘，避免了一个record的重复存储。
     * 当我们启动了幂等性，就能够保证producer往broker发送【一个】record时，可以以【exactly once】的语义进行数据传输，即既不会丢，也不会重复，一个record在broker中仅存储一次。
     */
    @Test
    public void testIdempotence() {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "idempotence-cli");
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(32).toBytes()));
        producerProperties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DataSize.ofMegabytes(64).toBytes()));
        // 启动幂等需要增加以下配置：
        // 1.enable.idempotence=true
        // 2.acks=all
        // 3.retries>0
        // 4.max.in.flight.requests.per.connection<=5
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        producerProperties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        producerProperties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(30).toMillis()));
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(Duration.ofSeconds(1).toMillis()));
        producerProperties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Duration.ofSeconds(29).toMillis()));

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties)) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("hello_idempotence", "hello", "world");
            Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(producerRecord);
            // 由于acks=all，因此只有在ISR的所有副本都写入完毕后，producer才会获得响应，get方法才会解除阻塞
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.printf("partition=%d,offset=%d,timestamp=%d%n",recordMetadata.partition(),recordMetadata.offset(),recordMetadata.timestamp());
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 事务配置总结
     * 1.在consume - transform - produce的consumer端
     * a) 关闭自动提交位点
     *      enable.auto.commit=false
     * b) 程序中也不要手动提交位点
     * <p>
     * 2.在consume - transform - produce的producer端
     * a) 启动幂等性功能：
     *      enable.idempotence=true
     *      acks=all
     *      retries>0
     *      max.in.flight.requests.per.connection<=5
     * b) 设置一个独一无二的事务id
     *      transactional.id=xxx
     * <p>
     * 3.在消费producer发送的topic的consumer端
     * a) 事务隔离性设置为read_committed
     *      isolation.level=read_committed
     * <p>
     * kafka事务与mysql事务的区别是：
     * kafka的事务是在producer客户端端生成的，不同的producer在往同一个topic中发送数据时，可以一个producer以事务的方式发送，另一个以非事务的方式发送。同时consumer在消费topic时，也可以选择读不读取事务中未提交的数据。
     * mysql事务是在mysql实例端的，客户端在插入数据时，是否启动事务也是由客户端决定。但在读取数据时，是由mysql实例决定事务的隔离级别，而不是客户端自行决定。
     */
    @Test
    public void testTransaction() {
         /*
             事务的典型应用模型是consume-transform-produce，也就是将数据拉取、数据加工、数据发送视作一个原子性操作，包含在一个事务里。只有这批数据全部处理完毕，才能事务提交。这批数据中任意一条数据发生处理问题，都会进行事务回滚。
             举例来说，程序中groupId=myGroup的consumer从topicA拉取到了R1、R2、R3这三条数据，然后对其进行数据加工，最终将这三条数据发送到topicB。
             只有在R1、R2、R3都发送到topicB了，才能提交事务，此时会：
             a) 将topicA中myGroup这个group的位点提交到R3的offset + 1
             b) 将topicB中R1、R2、R3置为已提交状态，这样topicB的consumer(read_committed)就可以拉取到这三条数据了
             如果R1已经处理完毕并发送到topicB，但在处理R2时发生了异常，此时会回滚事务，此时会：
             a) 不提交topicA中myGroup的位点，即使R1已经被读取并处理完了
             b) 将已经向topicB发送的R1这条数据置成回滚状态，这样topicB的consumer(read_committed)就不能拉取能R1这条数据了，即使R1实际已经发送到topicB中了

             幂等的启动保证了producer往一个topic的一个partition发送一个record时，既不会丢也不会重复。而事务则是保证一批数据在发往不同topic的不同partition，要么全部成功，要么全部失败，避免部分成功，部分失败，最终造成破坏【数据一致性】的问题
             什么时候需要事务：
             1.要往topic里发送一批数据。一次只发送一个record的话，只需要使用幂等性就可以了。
             2.这批数据具有【数据一致性】的特征。消费者不能只读取其中的一部分数据，需要消费者要么全部读取到，要么全都不读取。
             什么是数据一致性？
             比如有一笔A到B的转账，产生了两条数据，一个是A减少100，另一个是B增加100。这两条数据要么全部处理完，要么全部不处理，因此这两条数据具有数据一致性特征。假如消费者只读取到了A减少100，B增加100由于生产者处理出错而没有发出，那么A就白白减少了100，数据的一致性就被破坏了。
         */
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ctp-group");
        consumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "ctp-cli");
        // 在consume-transform-produce模式中：
        // 1.必须要关闭自动提交位点
        // 2.也不能在程序中手动提交位点
        // 因为需要在producer提交事务成功后，才提交位点
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");
        consumerProperties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "tx-producer");
        // 开启事务的前提时开启幂等性
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        producerProperties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "500");
        // 启动事务后还需要设置一个事务id，每个生产者的事务id应该都是独一无二的
        producerProperties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        producerProperties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "10000");

        KafkaProducer<String, String> kafkaProducer = null;
        KafkaConsumer<String, String> kafkaConsumer = null;
        // 下面这些代码主要有以下操作：
        // 1.consume:从hello_world中拉取一批数据
        // 2.transform:对这批数据进行逻辑处理
        // 3.produce:将处理结果发送只hello_tx
        // 使用事务后，上面这三个操作则被看作一个原子操作，只有所有数据全部处理成功才算成功，处理任意一个record发送错误，所有数据都会失败(包含已处理的数据)。
        try {
            kafkaProducer = new KafkaProducer<>(producerProperties);
            kafkaConsumer = new KafkaConsumer<>(consumerProperties);
            kafkaProducer.initTransactions();
            kafkaProducer.beginTransaction();
            kafkaConsumer.subscribe(Collections.singleton("hello_world"));
            // 从hello_world拉取一批数据
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2));
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(consumerRecords.partitions().size());
            for (ConsumerRecord<String, String> record : consumerRecords) {
                // 对拉取的数据进行处理
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("hello_tx", record.key(), String.format("%s#%s", record.value(), "_tx"));
                // 将处理后的数据发送至hello_tx
                kafkaProducer.send(producerRecord);
            }
            for (TopicPartition partition : consumerRecords.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = consumerRecords.records(partition);
                ConsumerRecord<String, String> lastRecord = partitionRecords.get(partitionRecords.size() - 1);
                offsets.put(partition, new OffsetAndMetadata(lastRecord.offset() + 1));
            }
            // 使用sendOffsetsToTransaction方法设置在事务提交后，consume - transform - produce中consumer需要提交的位点信息
            kafkaProducer.sendOffsetsToTransaction(offsets, kafkaConsumer.groupMetadata());
            // 提交事务，只有提交事务后，事务中发送的record才可以被consumer拉取到
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            // 当发生异常后，回滚事务，此时事务中已发送的record都不会被consumer拉取到
            if (Objects.nonNull(kafkaProducer)) {
                kafkaProducer.abortTransaction();
            }
        } finally {
            if (Objects.nonNull(kafkaProducer)) {
                kafkaProducer.close();
            }
            if (Objects.nonNull(kafkaConsumer)) {
                kafkaConsumer.close();
            }
        }
    }

    @Test
    public void testTransactionConsumer() {
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tx-group");
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "tx-client");
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 事务的处理不只包含生产者，也包含消费者。消费者需要设置事务隔离级别为read_committed，让消费者拉取不到未提交事务中发送的record。
        // consumer默认的事务隔离级别是read_uncommitted，所以生产者以事务的形式往一个topic里发送record时，所有consumer在消费这个topic也需要设置事务隔离级别为read_committed。
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerConfig)) {
            kafkaConsumer.subscribe(Collections.singleton("hello_tx"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(2));
                consumerRecords.forEach(System.out::println);
                kafkaConsumer.commitSync();
            }

        }
    }

    @Test
    public void testProduceRecords() {
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tx-group");
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "tx-client");
        consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 事务的处理不只包含生产者，也包含消费者。消费者需要设置事务隔离级别为read_committed，让消费者拉取不到未提交事务中发送的record。
        // consumer默认的事务隔离级别是read_uncommitted，所以生产者以事务的形式往一个topic里发送record时，所有consumer在消费这个topic也需要设置事务隔离级别为read_committed。
        consumerConfig.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


    }
}
