package com.mzq.hello.flink.usage.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;

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
        aggregateFunction1();
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
}
