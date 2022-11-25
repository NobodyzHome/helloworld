package com.mzq.hello.flink.usage.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.fs.FileSystem;

public class HiveUsage extends BaseSqlUsage {
    @Override
    public void execute() {
        testHive();
//        testHbase();
    }

    private void testHive() {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        HiveCatalog hiveCatalog = new HiveCatalog("my-hive", "default", "/Users/maziqiang/Documents/my-libs");
        tableEnvironment.registerCatalog("my-hive", hiveCatalog);
        tableEnvironment.useCatalog("my-hive");
        tableEnvironment.executeSql("create table default_catalog.default_database.kafka_sink(dept_no string,dept_name string,cnt bigint)" +
                " with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092','properties.group.id'='my-group','topic'='hello_world','format'='json')");

        tableEnvironment.executeSql("insert into default_catalog.default_database.kafka_sink select dept_no,dept_name,count(emp_no) cnt from employee group by dept_no,dept_name");
    }

    private void testHbase() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.inBatchMode();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);
        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "default", "/Users/maziqiang/Documents/my-libs");
        tableEnvironment.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnvironment.useCatalog(hiveCatalog.getName());
//        tableEnvironment.executeSql("create table ")

    }
}
