package com.mzq.hello.flink;

import com.mzq.hello.domain.BdWaybillOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * ConsumerInterceptor用于对KafkaConsumer的poll、commit等方法，在获取到broker返回的响应后进行的拦截
 */
@Slf4j
public class BdWaybillInfoConsumerInterceptor implements ConsumerInterceptor<String, BdWaybillOrder> {

    /**
     * 在kafkaConsumer的poll方法被调用，从broker拉取到数据后，在poll方法内会调用该方法来获取数据
     * 我们可以使用该方法来修改拉取到的数据，这里只是演示，但生产环境非常不建议这么做。会增加排查问题的难度：出问题的数据是kafka本来的数据，还是经过拦截器修改的数据？
     *
     * 虽然该方法是在poll方法内部被执行的，但当执行该方法时，consumer已经开始针对max.poll.interval.ms进行计时了，因此如果这里面的逻辑过重或需要连接外部设备（redis等），
     * 有可能导致来不及进行下一次poll，就已经超过max.poll.interval.ms的配置了。下一次poll的话，consumer又要重新和broker建立连接，触发rebalance，使消费性能非常低（要知道，在rebalance执行过程中，group中的所有consumer都无法进行数据拉取）。
     *
     * @param records 从broker拉取的数据
     * @return 返回给用户的数据
     */
    @Override
    public ConsumerRecords<String, BdWaybillOrder> onConsume(ConsumerRecords<String, BdWaybillOrder> records) {
        for (ConsumerRecord<String, BdWaybillOrder> consumerRecord : records) {
            BdWaybillOrder bdWaybillOrder = consumerRecord.value();
            String waybillCode = bdWaybillOrder.getWaybillCode();
            String newWaybillCode = "new-" + waybillCode;
            bdWaybillOrder.setWaybillCode(newWaybillCode);
            log.info("已修复拉取到的BdWaybillOrder的数据，waybillCode={}，newWaybillCode={}", waybillCode, newWaybillCode);
        }
        return records;
    }

    /**
     * 该方法在KafkaConsumer调用commit方法，发送Commit请求并且得到broker的响应后，才会被调用
     *
     * @param offsets 每个分区提交的位点信息
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.info("位点提交信息：{}", offsets);
    }

    /**
     * 该方法会在KafkaConsumer关闭时（调用close方法）被调用
     */
    @Override
    public void close() {
        log.info("kafka consumer即将被关闭！");
    }

    /**
     * 该方法在new KafkaConsumer时被调用，传入的configs参数就是传给KafkaConsumer的配置
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        log.info("consumer配置如下：{}", configs);
    }
}
