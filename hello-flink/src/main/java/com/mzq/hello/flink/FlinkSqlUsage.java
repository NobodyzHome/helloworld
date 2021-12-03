package com.mzq.hello.flink;

import com.mzq.hello.flink.usage.sql.UdfFunctionUsage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlUsage {

    /**
     * flink sql的核心思想是从数据源拉取数据，形成source table，然后对table进行的map,filter等操作就是flink内部的事情了（ETL中的T,transform），
     * 在flink sql中可以对table进行map（通过函数）、filter（通过where）等操作，也可以对多个table（也就是多个数据源）进行join操作，然后生成出一个新的view。
     * 通过对source table进行处理，形成一个新的table（也就是view），再对新的table进行处理，再生成新的table，最后将处理完毕的table的数据插入到sink table，就形成了整个flink DAG。
     * 想象一下，我们通过代码写flink程序，其实不也是先写个source，然后把source交给filter function处理形成数据流A，然后再把数据流A交给map function处理，形成数据流C，最后把数据流C交给sink function处理，形成了一个DAG图，
     * 原理是一样的。只不过flink sql用更抽象的概念，隐去了SourceFunction、MapFunction、FilterFunction、SinkFunction，取而代之的是数据人员更熟悉的create table、concat(username,'_test')、where、insert into sink_table select * from view。
     *
     * @param args
     */
    public static void main(String[] args) {
//        AggregationUsage aggregationUsage = new AggregationUsage();
//        aggregationUsage.execute();

        UdfFunctionUsage udfFunctionUsage = new UdfFunctionUsage();
        udfFunctionUsage.execute();
    }

    public static void test1() {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);

//        StringDebeziumDeserializationSchema stringDebeziumDeserializationSchema = new StringDebeziumDeserializationSchema();
//        MySqlSource.Builder<String> source = MySqlSource.builder();
//        source.hostname("localhost").port(3306).serverId(123456).databaseList("hello_database").tableList("hello_database.hello_world").username("user_binlog").password("123456").deserializer(stringDebeziumDeserializationSchema);
//        DataStreamSource<String> streamSource = streamExecutionEnvironment.addSource(source.build());
//        DataStreamSink<String> print = streamSource.print().setParallelism(1);
//        streamExecutionEnvironment.execute();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注意：在sql配置mysql-cdc connector时，table-name属性不要带数据库名（在这里就是hello_world），而在编程方式赋值tableList属性时，要带着数据库名（在这里就是hello_database.hello_world）
        tEnv.executeSql("CREATE TABLE mysql_binlog (id INT NOT NULL,name string) WITH ('connector' = 'mysql-cdc', 'hostname' = 'my-mysql'" +
                ", 'port' = '3306', 'username' = 'user_binlog', 'password' = '123456', 'database-name' = 'hello_database', 'table-name' = 'hello_world'" +
                ",'scan.incremental.snapshot.enabled'='false')");
        // 创建一个视图，对原数据进行转换
        tEnv.executeSql("CREATE view mysql_binlog_view (id,name) as select id,concat(name,'#test') from mysql_binlog");
        // 创建一个视图，里面存有测试数据
        tEnv.executeSql("create view sex_info(id,sex) as select id,sex from (values(5,'male'),(6,'female')) as test_table(id,sex)");
        // 创建一个filesystem table，从文件中获取table的数据。由于format=json，因此是尝试将文件的每一行数据按照json的格式进行解析，获取id和sex两个属性
//        tEnv.executeSql("create table sex_info(id int,sex string) with ('connector'='filesystem','path'='file:///my-files/flink','format'='json')");
        // 创建一个sink table，将table中的数据输出到console
        tEnv.executeSql("create table print_sink (id int,name string,sex string) with ('connector'='print')");
//        tEnv.executeSql("CREATE TABLE jdbc_sink (id int primary key,name string) WITH (\n" +
//                "   'connector' = 'jdbc',\n" +
//                "   'url' = 'jdbc:mysql://my-mysql:3306/hello_database',\n" +
//                "   'table-name' = 'hello_world_copy',\n" +
//                "   'username' = 'root',\n" +
//                "   'password' = '123456'\n" +
//                ")");
//        tEnv.executeSql("CREATE TABLE kafka_sink (id int primary key,name string) WITH (\n" +
//                "   'connector' = 'kafka',\n" +
//                "   'topic' = 'hello_world',\n" +
//                "   'value.format' = 'debezium-json',\n" +
//                "   'properties.bootstrap.servers' = 'kafka-1:9092'\n" +
//                ")");
//        tEnv.executeSql("create table elasticsearch_sink (id int primary key,name string,sex string) with ('connector'='elasticsearch-7'," +
//                "   'hosts'='my-elasticsearch:9200'," +
//                "   'index'='hello_world'" +
//                ")");
        // 将mysql_binlog_view和test_data这两个视图使用id进行join，将join结果写入到print_sink中
        tEnv.executeSql("INSERT INTO print_sink SELECT mysql_binlog_view.id,name,sex FROM mysql_binlog_view join sex_info on mysql_binlog_view.id=sex_info.id");
//        tEnv.executeSql("INSERT INTO jdbc_sink SELECT id,CONCAT(name,'_test') FROM mysql_binlog_view where id>3");
//        tEnv.executeSql("INSERT INTO kafka_sink SELECT id,CONCAT(name,'_test') FROM mysql_binlog_view where id>3");
//        tEnv.executeSql("INSERT INTO elasticsearch_sink SELECT id,CONCAT(name,'_test') FROM mysql_binlog_view where id>3");
    }

    public static void test2() {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(5);

        TableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        tableEnvironment.executeSql("create table hello_world(id int primary key,name string) with('connector'='kafka','topic'='hello_database','properties.bootstrap.servers'='kafka-1:9092','scan.startup.mode'='latest-offset','format'='canal-json','canal-json.table.include'='hello_world')");
        tableEnvironment.executeSql("create table print_sink(id int primary key,name string,alias string) with('connector'='print','sink.parallelism'='10')");
        tableEnvironment.executeSql("create view test_view(id,name) as select id,name from (values (1,'aiyaya'),(2,'wahaha')) as t (id,name)");
        tableEnvironment.executeSql("create view test(id,name,test_name) as select hello_world.id,hello_world.name,test_view.name from hello_world left join test_view on hello_world.id=test_view.id");
        tableEnvironment.executeSql("create view test1(id,name,test_name) as select id,name,concat(test_name,'#hoho') from test");
        tableEnvironment.executeSql("insert into print_sink select  id,name,test_name  from test1");
    }

    public static void test4() {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        TableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
//        TableResult tableResult = tableEnvironment.executeSql("create view test(id,name) as select id,name from (values (1,'hello'),(2,'world')) as t(id,name)");
        tableEnvironment.executeSql("create table es_sink(id string primary key,name string,proc_time as PROCTIME()) with('connector'='elasticsearch-7','hosts'='http://localhost:9200','index'='test_index','format'='json')");
        tableEnvironment.executeSql("insert into es_sink(id) values('1')");
    }

    public static void test3() {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);

        // 在jobManager执行该方法时，会立即向jobManager注册一个仅属于当前会话的source table
        tableEnvironment.executeSql("create table student(id int primary key,name string,age int,birthday timestamp,teacher_id int) with(" +
                "'connector'='mysql-cdc'," +
                "'hostname'='localhost'," +
                "'port'='3306'," +
                "'username'='user_binlog'," +
                "'password'='123456'," +
                "'database-name'='hello_database'," +
                "'table-name'='student'," +
                "'scan.incremental.snapshot.enabled'='false'" +
                ")");

        tableEnvironment.executeSql("create table teacher(id int primary key,name string,age int,level int) with(" +
                "'connector'='mysql-cdc'," +
                "'hostname'='localhost'," +
                "'port'='3306'," +
                "'username'='user_binlog'," +
                "'password'='123456'," +
                "'database-name'='hello_database'," +
                "'table-name'='teacher'," +
                "'scan.incremental.snapshot.enabled'='false'" +
                ")");

        // 在jobManager执行该方法时，会立即向jobManager注册一个仅属于当前会话的view
        // 这句话的意思是创建一个view，使用jobManger中student表和teacher表进行内连接，将匹配的数据输出到下游算子
        tableEnvironment.executeSql("create view student_teacher(stu_id,stu_name,stu_age,stu_birthday,tea_id,tea_name,tea_age,tea_level) as " +
                "select student.id,student.name,student.age,student.birthday,teacher.id,teacher.name,teacher.age,teacher.level from student full outer join teacher on student.teacher_id=teacher.id");

        // 创建一个sink table，向console输出结果
        tableEnvironment.executeSql("create table print_sink(stu_id int,stu_name string,stu_age int ,stu_birthday timestamp,tea_id int,tea_name string,tea_age int,tea_level int) with('connector'='print')");
//        tableEnvironment.executeSql("create table print_summary_sink(teacher_id int,teacher_name string,student_count BIGINT) with('connector'='print')");
        tableEnvironment.executeSql("create table elasticsearch_summary_sink(teacher_id int primary key,teacher_name string,student_count BIGINT) with('connector'='elasticsearch-7'," +
                "'hosts'='http://localhost:9200'," +
                "'index'='teacher_summary'" +
                ")");

//        tableEnvironment.executeSql("insert into print_summary_sink select tea_id,tea_name,count(stu_id) from student_teacher group by tea_id,tea_name");
        tableEnvironment.executeSql("insert into elasticsearch_summary_sink select teacher.id,teacher.name,count(student.id) from teacher left join student on teacher.id=student.teacher_id group by teacher.id,teacher.name");
        // 执行这句话时，flink任务需要的source和sink都全了（也就是从哪儿拉取数据，数据又写到哪儿），这时他就会开始生成DAG图，然后来把算子分发给taskManager
        tableEnvironment.executeSql("insert into print_sink select * from student_teacher");
    }


}