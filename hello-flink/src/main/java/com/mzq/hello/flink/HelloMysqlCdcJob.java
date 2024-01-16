package com.mzq.hello.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class HelloMysqlCdcJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment=StreamExecutionEnvironment.createLocalEnvironment(1);
        streamExecutionEnvironment.enableCheckpointing(Duration.ofSeconds(30).toMillis(), CheckpointingMode.EXACTLY_ONCE);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test_database") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("test_database.test_table") // 设置捕获的表
                .username("root")
                .password("123456")
                .serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .includeSchemaChanges(true)
                .build();
        DataStreamSource<String> stringDataStreamSource = streamExecutionEnvironment.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"test123");
        stringDataStreamSource.print();

        streamExecutionEnvironment.execute();
    }
}
