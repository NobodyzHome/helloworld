package com.mzq.hello.flink.usage.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HudiUsage extends BaseSqlUsage {
    @Override
    public void execute() {
        testHudi();
    }

    private void testHudi() {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tableEnvironment.executeSql("" +
                "CREATE TABLE hudi_hello_world(\n" +
                "                   id VARCHAR(20) PRIMARY KEY NOT ENFORCED,\n" +
                "                   name VARCHAR(10),\n" +
                "                   age INT,\n" +
                "                   ts bigint,\n" +
                "                   `dt` VARCHAR(20)\n" +
                ")\n" +
                "    PARTITIONED BY (`dt`)\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = 'hdfs://namenode:9000/hudi/app.dev/hello_world_1',\n" +
                "  'payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',\n" +
                "  'hoodie.payload.ordering.field' = 'ts',\n" +
                "  'table.type' = 'MERGE_ON_READ' -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE\n" +
                ")" +
                "");
        tableEnvironment.executeSql("insert into hudi_hello_world values('100',cast(null as string),15,100,'2023-05-15'),('100','zhaowu',cast(null as int),100,'2023-05-15');");
    }

}
