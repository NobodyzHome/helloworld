package com.mzq.hello.flink;

import com.mzq.hello.domain.BdWaybillOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

@Slf4j
public class BdWaybillInfoConsumerInterceptor implements ConsumerInterceptor<String, BdWaybillOrder> {

    /**
     * 在消费到数据时
     *
     * @param records
     * @return
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

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("consumer配置如下：{}", configs);
    }
}
