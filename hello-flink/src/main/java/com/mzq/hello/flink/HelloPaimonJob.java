package com.mzq.hello.flink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class HelloPaimonJob {

    public static void main(String[] args) {
        StreamExecutionEnvironment streamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
//        streamExecutionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        streamExecutionEnvironment.enableCheckpointing(Duration.ofSeconds(30).toMillis(), CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment streamTableEnvironment=StreamTableEnvironment.create(streamExecutionEnvironment);
        streamTableEnvironment.executeSql("CREATE CATALOG paimon_catalog WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    -- 元数据存储方式，可选值为filesystem和hive，默认是filesystem，也就是将元数据存储到文件系统中。如果为hive的话，则是将元数据发送至hive的metastore服务。\n" +
                "    'metastore' = 'filesystem',\n" +
                "    -- 如果metastore为filesystem，元数据存储的目录\n" +
                "    'warehouse' = '/Users/maziqiang/Documents/my-paimon',\n" +
                "    -- 默认使用的数据库。注意：paimon会自动在warehouse指定的目录下创建数据库的目录，创建的目录为【数据库名.db】。因此，这里指定的database不用带着.db后缀，否则创建的数据库目录就变为了app.db.db(假设database=app.db)。这点和hudi不同，hudi创建的数据库目录就是数据库名本身。\n" +
                "    'default-database'='app'\n" +
                ")");
        streamTableEnvironment.executeSql("use catalog paimon_catalog");
//        streamTableEnvironment.executeSql("select * from paimon_none_changelog where dt='2024-02-02' and age<20");
        streamTableEnvironment.executeSql("insert into paimon_none_changelog values(1,'hello',32,'2024-01-20')");

    }
}
