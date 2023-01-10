package com.mzq.hello.flink.usage.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.fs.FileSystem;

public class HiveUsage extends BaseSqlUsage {
    @Override
    public void execute() {
//        testHive();
        testHbase();
    }

    private void testHive() {
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        HiveCatalog hiveCatalog = new HiveCatalog("my-hive", "default", "/my-repository");
        tableEnvironment.registerCatalog("my-hive", hiveCatalog);
        tableEnvironment.useCatalog("my-hive");
        tableEnvironment.executeSql("create table default_catalog.default_database.kafka_sink(dept_no string,dept_name string,cnt bigint)" +
                " with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092','properties.group.id'='my-group','topic'='hello_world','format'='json')");

        tableEnvironment.executeSql("insert into default_catalog.default_database.kafka_sink select dept_no,dept_name,count(emp_no) cnt from employee group by dept_no,dept_name");
    }

    private void testHbase() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.inBatchMode();
        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);
        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "default", "/my-repository");
        tableEnvironment.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnvironment.useCatalog(hiveCatalog.getName());

        tableEnvironment.executeSql("create table if not exists hbase_emp_usage(\n" +
                "    rowkey string,\n" +
                "    info row<emp_no string,emp_name string,sex string,dept_no string,dept_name string,create_dt string,salary string>\n" +
                ")\n" +
                "with(\n" +
                "    'connector'='hbase-1.4',\n" +
                "    'zookeeper.quorum'='zookeeper:2181',\n" +
                "    'table-name'='emp_usage'\n" +
                "    )");
        tableEnvironment.executeSql("insert into hbase_emp_usage\n" +
                "select\n" +
                "    emp_no,\n" +
                "    row(emp_no,emp_name,sex,dept_no,dept_name,create_dt,salary_str) info\n" +
                "from (\n" +
                "    select *,cast(salary as string) salary_str\n" +
                "    from employee\n" +
                ")");
    }
}
