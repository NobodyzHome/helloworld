package com.mzq.hello.flink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

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
     */
    @Test
    public void test1() throws ExecutionException, InterruptedException, TimeoutException {
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
        // RecordAccumulator中存储的每一个ProducerBatch的最大容量。当发来一个ProducerRecord时，如果它的大小比小batch.size小，那么会尝试把它放到已有的ProducerBatch中。
        // 否则会创建一个ProducerBatch，它的大小和ProducerRecord的大小一样，但注意这种超过batch.size的ProducerBatch不会放到对象池中进行对象服用
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(10).toBytes()));
        // send、partitionsFor等方法阻塞用户线程（因为它们的执行是在用户线程中执行的）的最大时间，当这些方法执行时间超过该配置，用户线程则会收到TimeoutException异常，以此来解除用户线程的阻塞
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Duration.ofSeconds(3).toMillis()));
        // RecordAccumulator中存储待发送数据的最大内存。如果RecordAccumulator中存储的ProducerBatch容量已超过该配置，当新插入一条ProducerRecord时，那么当前线程会阻塞，等待RecordAccumulator中有请求被响应，从而从RecordAccumulator中提出，腾出足够的空余的容量
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DataSize.ofMegabytes(2).toBytes()));
        // max.request.size用于控制ProducerRecord的最大大小（单位是byte），当ProducerRecord经过序列化器序列化后，key和value的大小的和，再加上一些请求需要的信息，构成了该请求的大小。如果该大小超过了配置，就会报RecordTooLargeException异常，这个请求都不会写入到RecordAccumulator中。
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(DataSize.ofKilobytes(5).toBytes()));
        /*
         * 该参数是发送给broker，broker收到该参数后，会根据参数的值决定【何时】对发来的请求进行响应：
         * 1.ack=0，broker在收到请求后，就立即对请求给出响应。这种情况下，send方法返回的RecordMetadata中，offset就固定为-1，因为响应结果是不等数据写入到副本后就发送了。
         * 2.ack=1，broker在收到请求后，会将数据写入到leader副本，然后就对请求给出响应。因此在此时，send方法返回的RecordMetadata中，offset就是实际的数据写入到副本中的offset。
         * 3.ack=all或-1，broker在收到该请求后，会将数据写入到leader副本，然后等待ISR中的其他broker同步完该条数据，才会给出响应。因此send方法返回的RecordMetadata中也会带有数据写入到副本的实际的offset。
         *
         * 注意：
         * 无论ack等于几，都是Sender线程接收来自kafka的响应，然后根据响应内容把Future<RecordMetadata>设置为done状态，这样用户线程就可以获取到kafka的响应结果了（在此之前调用get方法会被阻塞）
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
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5");
        /*
         * Sender线程在从RecordAccumulator中拉取到ProducerBatch后，将ProducerBatch发送到broker前，会判断当前ProducerBatch是否过期了，就是使用【当前时间 - ProducerBatch的创建时间 > delivery.timeout.ms】来判断是否过期。
         * 如果有过期，则Sender线程会抛出异常。相应地，主线程调用Future.get方法时也会收到异常(异常内容：Expiring 1 record(s) for hello-world-0:11 ms has passed since batch creation)。
         * 发生该异常主要原因是：生产速度大于Sender线程拉取ProducerBatch并发送至broker的速度，导致ProducerBatch在RecordAccumulator中驻留的时间超过了delivery.timeout.ms的配置。
         * 如果生产速度过高，我们可以提高该配置，降低ProducerBatch过期几率。
         */
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "150");

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
        // recordMetaDataResultFuture返回的是broker对该请求的响应结果，在服务器给出响应前，调用get方法会被阻塞。而Sender线程在收到响应后，会把该Future设置为done，这样用户线程就可以解除阻塞，获取服务器的响应数据了。
        RecordMetadata recordMetadata = recordMetaDataResultFuture.get();
        // 主线程获取服务器的响应结果并使用
        System.out.println(recordMetadata);

        Thread.sleep(5000);

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
    public void testSendWithCallback() {

    }

    public void testInterceptor() {

    }

    public void testSerializer() {

    }

    public void testPartitioner() {

    }
}
