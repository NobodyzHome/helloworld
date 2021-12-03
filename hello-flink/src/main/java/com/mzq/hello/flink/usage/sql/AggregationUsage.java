package com.mzq.hello.flink.usage.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;

public class AggregationUsage extends BaseSqlUsage {

    @Override
    public void execute() {
        windowAggregation();
    }

    public void windowAggregation() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.mini-batch.enabled", "false");
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
        configuration.setString("table.exec.mini-batch.size", "1000");
        configuration.setString("table.exec.resource.default-parallelism", "1");
        configuration.setString("execution.checkpointing.interval", "10 s");
        configuration.setString("execution.checkpointing.timeout", "50 s");
        configuration.setString("execution.checkpointing.unaligned", "true");
        configuration.setString("execution.checkpointing.aligned-checkpoint-timeout", "200 ms");
        configuration.setString("execution.checkpointing.unaligned.forced", "false");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        // 创建source table
        tableEnvironment.executeSql("create table hello_world(id string,name string,proc_time as PROCTIME()) with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092','topic'='hello_world'" +
                ",'scan.startup.mode'='earliest-offset','key.format'='raw','key.fields'='id','value.format'='json')");
        // 创建sink table
        tableEnvironment.executeSql("create table es_hello_world_window_aggregation(\n" +
                "    id string primary key,\n" +
                "    window_start timestamp(3),\n" +
                "    window_end timestamp(3),\n" +
                "    name_count BIGINT,\n" +
                "    concat_name string\n" +
                ") with(\n" +
                "    'connector'='elasticsearch-7',\n" +
                "    'hosts'='http://my-elasticsearch:9200',\n" +
                "    'index'='hello_world_window_aggregation',\n" +
                "    'format'='json'\n" +
                ")");
        // 对source table进行window聚合，将聚合结果写入到es
        tableEnvironment.executeSql("insert into es_hello_world_window_aggregation " +
                "select id,window_start,window_end,count(name) name_count,listagg(name) concat_name " +
                "from table(tumble(table hello_world,descriptor(proc_time),interval '10' seconds)) " +
                "group by id,window_start,window_end");
    }
}
