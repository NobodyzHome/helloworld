package com.mzq.hello.flink.usage.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UdfFunctionUsage extends BaseSqlUsage {

    @Override
    public void execute() {
//        basicTableFunction();
//        tableFunctionWithParam();
//        innerJoinTableFunction();
//        leftOuterJoinTableFunction();
//        scalarFunction();
//        scalarFunction1();
//        aggregateFunction();
//        aggregateFunction1();
//        test1();
//        test2();
//        test3();
        test5();
    }

    public void basicTableFunction() {
        Configuration configuration = new Configuration();
        // 在任务配置中设置global parameter，key为pipeline.global-job-parameters，value是一个map，map的格式为：key1:value1,key2:value2。
        // 其中如果value1或value2有关键字":"，那么需要用单引号给括上，例如redis.url这个key，value为redis://localhost:6379，包含关键字':'，所以应该配置为redis.url:'redis://localhost:6379'
        configuration.setString("pipeline.global-job-parameters", "redis.url:'redis://localhost:6379',my-param1:hello_world");
        configuration.setString("table.exec.resource.default-parallelism", "1");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        // 我们也可以通过编码的方式，给任务增加global parameter
        tableEnvironment.getConfig().addJobParameter("my-param2", "hello_nancy");
        // 注册function
        tableEnvironment.executeSql("create function generate_rows as 'com.mzq.hello.flink.sql.udf.table.GenerateRows'");
        tableEnvironment.executeSql("create table print_sink(id int,name string,param1 string,param2 string) with('connector'='print')");
        // 使用table function生成表数据，然后插入到print_sink表中
        tableEnvironment.executeSql("insert into print_sink select * from table(generate_rows())");
    }

    public void tableFunctionWithParam() {
        Configuration configuration = new Configuration();
        configuration.setString("pipeline.global-job-parameters", "my-param1:hello,my-param2:world");
        configuration.setString("table.exec.resource.default-parallelism", "1");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("create function generate_rows as 'com.mzq.hello.flink.sql.udf.table.GenerateRows'");
        tableEnvironment.executeSql("create table print_sink(id int,name string,param1 string,param2 string) with('connector'='print')");
        // 调用table function时传入参数
        tableEnvironment.executeSql("insert into print_sink select * from table(generate_rows(15,'test-'))");
    }

    public void innerJoinTableFunction() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");
        configuration.setString("pipeline.global-job-parameters", "redis.url:'redis://localhost:6379'");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("create function alias_search as 'com.mzq.hello.flink.sql.udf.table.AliasRedisSearch'");
        tableEnvironment.executeSql("create table print_sink(id int,name string,alias string,alias_key string) with('connector'='print')");
        tableEnvironment.executeSql("insert into print_sink select student.id,student.name,alias.name,alias.key from " +
                // 使用左表和table function进行内连接，如果table function中没有返回对应的数据，则左表对应的行就不显示
                "(select id,name from (values(1,'zhangsan'),(2,'lisi')) as t(id,name)) as student," +
                "lateral table(alias_search(student.id)) as alias");
    }

    public void leftOuterJoinTableFunction() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");
        configuration.setString("pipeline.global-job-parameters", "redis.url:'redis://localhost:6379'");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("create table print_sink(id int,name string,alias string,alias_key string) with('connector'='print')");
        tableEnvironment.executeSql("create function alias_search as 'com.mzq.hello.flink.sql.udf.table.AliasRedisSearch'");
        tableEnvironment.executeSql("insert into print_sink " +
                // 使用左外连接的方式和table function的表进行连接，即使table function没有返回对应的数据，左表的数据也可以正常显示
                "select student.id,student.name,my_alias.alias_name,my_alias.alias_key from (select * from (values(1,'zhangsan'),(2,'lisi')) as my_table(id,name)) student " +
                // 在使用table function时，同时重新设置table function创造出来的表的表名和字段名
                "left join lateral table(alias_search(student.id)) as my_alias(id,alias_key,alias_name) on true");
    }

    public void scalarFunction() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");
        configuration.setString("pipeline.global-job-parameters", "redis.url:'redis://localhost:6379'");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("create table print_sink(id int,name string,alias string) with ('connector'='print')");
        tableEnvironment.executeSql("create function query_alias as 'com.mzq.hello.flink.sql.udf.scalar.AliasQuery'");
        tableEnvironment.executeSql("insert into print_sink select id,name,query_alias(id,name) alias from (values(1,'hello'),(2,'world')) as student(id,name)");
    }

    public void scalarFunction1() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");
        configuration.setString("pipeline.global-job-parameters", "redis.url:'redis://localhost:6379'");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("create table print_sink(id int,name string,use_which string,score DECIMAL(5,2)) with('connector'='print')");
        tableEnvironment.executeSql("create function redis_query as 'com.mzq.hello.flink.sql.udf.scalar.RedisQuery'");
        tableEnvironment.executeSql("insert into print_sink select id,name,search_result.use_which,search_result.score from (select id,name,redis_query(id,name) search_result from (values(1,'hello'),(2,'world')) as t(id,name))");
    }

    public void aggregateFunction() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("create function collect_set as 'com.mzq.hello.flink.sql.udf.aggregation.CollectUniqueStringAggregate'");
        tableEnvironment.executeSql("create table print_sink(id int,name string,nameArray array<string>) with('connector'='print')");
        tableEnvironment.executeSql("insert into print_sink select id,joined.joined,joined.uniqueStrArray " +
                "from (select id,collect_set(name,alias) joined from (select * from (values(1,'hello','haha'),(1,'world','heihei')) as t(id,name,alias)) group by id)");
    }

    public void aggregateFunction1() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");
        configuration.setString("pipeline.global-job-parameters", "redis.url:'redis://localhost:6379'");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("create table print_sink(id int,name string) with('connector'='print')");
        tableEnvironment.executeSql("create function alias_concat as 'com.mzq.hello.flink.sql.udf.aggregation.AliasSearchAggregate'");
        tableEnvironment.executeSql("insert into print_sink select id,alias_concat(name) alias_concat from (select id,name from (values(1,'hello'),(1,'world'),(2,'zhangsan'),(2,'lisi')) as t(id,name)) group by id");
    }

    public void test1() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("CREATE TABLE orders (\n" +
                " id INT primary key,\n" +
                " name STRING,\n" +
                " description STRING,\n" +
                " order_time timestamp(3),\n" +
                " update_time timestamp(3),\n" +
                " watermark for update_time as update_time - interval '30' second\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'my-mysql',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'user_binlog',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'hello_database',\n" +
                " 'table-name' = 'orders'\n" +
                ")");

        tableEnvironment.executeSql("create table print_sink(id int,name string) with ('connector'='print')");
        tableEnvironment.executeSql("insert into print_sink select id,max(name) from orders group by id");
    }

    public void test2() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("CREATE TABLE orders (\n" +
                " id INT primary key,\n" +
                " name STRING,\n" +
                " description STRING,\n" +
                " order_time timestamp(3),\n" +
                " update_time timestamp(3),\n" +
                " watermark for update_time as update_time - interval '30' second\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'localhost',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'user_binlog',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'hello_database',\n" +
                " 'table-name' = 'orders'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE shipment (\n" +
                " id INT primary key,\n" +
                " name STRING,\n" +
                " ship_time timestamp(3),\n" +
                " order_id int,\n" +
                " update_time timestamp(3),\n" +
                " watermark for update_time as update_time - interval '30' second\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'localhost',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'user_binlog',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'hello_database',\n" +
                " 'table-name' = 'shipment'\n" +
                ")");

        tableEnvironment.executeSql("create table print_sink(id int,name string) with('connector'='print')");
        tableEnvironment.executeSql("insert into print_sink SELECT o.id,s.name\n" +
                "FROM orders o , shipment s\n" +
                "where  o.id = s.order_id\n" +
                "AND o.update_time BETWEEN s.update_time - INTERVAL '20' HOUR AND s.ship_time");
    }

    public void hello_world() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");
        configuration.setString("table.exec.state.ttl", "30 min");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        // 向jm中注册source table
        tableEnvironment.executeSql("create table hello_world(id string,\n" +
                "                        name string,\n" +
                "                        update_time timestamp(3),\n" +
                "                        watermark for update_time as update_time - interval '30' second)\n" +
                "                        with('connector'='kafka','properties.bootfstrap.servers'='kafka-1:9092'\n" +
                "                            ,'topic'='hello_world'\n" +
                "                            ,'scan.startup.mode'='earliest-offset'\n" +
                "                            ,'key.format'='raw'\n" +
                "                            ,'key.fields'='id'\n" +
                "                            ,'value.format'='json')");
        // 向jm中注册sink table
        tableEnvironment.executeSql("create table es_sink(\n" +
                "                           id string primary key,\n" +
                "                           name string,\n" +
                "                           update_time timestamp(3)\n" +
                "                       )with('connector'='elasticsearch-7'\n" +
                "                           ,'hosts'='http://my-elasticsearch:9200'\n" +
                "                           ,'index'='hello_world'\n" +
                "                           ,'format'='json')");
        // 提交flink任务
        tableEnvironment.executeSql("insert into es_sink\n" +
                "select id,listagg(concat(name,'-helloworld')) concat_name,max(update_time)\n" +
                "from hello_world\n" +
                "where cast(id as int)>=15\n" +
                "group by id");
        // 提交flink任务
        tableEnvironment.executeSql("insert into hello_world values('15','hello',TIMESTAMP '2021-12-08 08:35:20'),('15','world',TIMESTAMP '2021-12-08 08:35:25')" +
                ",('16','zhangsan',TIMESTAMP '2021-12-08 09:40:05'),('17','lisi',TIMESTAMP '2021-10-08 09:50:03')");
    }

    public static void test5() {
        Configuration configuration = new Configuration();
        configuration.setString("table.exec.resource.default-parallelism", "1");

        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);
        tableEnvironment.executeSql("create view view_1 as select id,name from (values(1,'zhangsan'),(2,'lisi')) as t(id,name)");
        tableEnvironment.executeSql("create view view_2 as select id,alias from (values(1,'zs'),(1,'test'),(2,'lisi')) as t(id,alias)");
        tableEnvironment.executeSql("create table print_table(id int,name string,alias string) with('connector'='print')");
        tableEnvironment.executeSql("insert into print_table select t1.id,t1.name,t2.alias from view_1 t1 left join view_2 t2 on t1.id=t2.id");
    }
}
