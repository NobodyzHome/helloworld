package com.mzq.hello.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkSqlConnectorUsage {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setString("parallelism.default","1");
        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);

//        tableEnvironment.executeSql("create table lookup_table(id int,name string,rst string) with('connector'='testLookup','table-name'='hello_lookup')");
//        tableEnvironment.executeSql("create table sink_table(id int,name string,rst string) with('connector'='printJson','format'='json')");
//        tableEnvironment.executeSql("insert into sink_table " +
//                "select t1.id,t1.name,lt.rst from (select * from (values(1,'aa',proctime()),(2,'ba',proctime()),(3,'ca',proctime())) as t(id,name,row_time)) t1 left join lookup_table for system_time as of t1.row_time as lt on t1.name=lt.name and t1.id=lt.id");

        tableEnvironment.executeSql("create table redis_sink(ttl bigint metadata from 'the_ttl',jim_key string,jim_value string) with('connector'='redis','redis.url'='redis://localhost:6379')");
        tableEnvironment.executeSql("create table data_source(key string,the_value string) with('connector'='datagen','number-of-rows'='100','fields.key.length'='5','fields.the_value.length'='10')");
        tableEnvironment.executeSql("insert into redis_sink select 2000,* from data_source");
    }
}
