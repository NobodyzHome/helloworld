package com.mzq.hello.flink;

import com.mzq.hello.domain.WaybillRouteLink;
import com.mzq.hello.flink.func.source.WaybillRouteLinkSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.data.RowData;

import java.util.concurrent.TimeUnit;

/**
 * 我们知道flink是一种ETL pipeline的实现。我们flink程序的整体流程是从source中拉取数据，然后提取数据中的字段并进行一些转换，最后把这些字段存储到数据库中。这是一个最简单的flink程序的步骤，涉及到三个算子：source -> map -> sink。
 * One very common use case for Apache Flink is to implement ETL (extract, transform, load) pipelines that take data from one or more sources, perform some transformations and/or enrichments, and then store the results somewhere.
 * <p>
 * 我们对上面这个过程抽象一下，我们可以把source想象成一个table。假设source中的数据有两个字段id,name，并且是json格式的，现在假设source中有两个数据：
 * {"id":1,"name":"张三"},{"id":2,"name":"李四"}
 * 我们可以把这个source想象成以下的table：
 * -------------
 * ｜id ｜name ｜
 * -------------
 * ｜1  ｜ 张三 ｜
 * -------------
 * ｜2  ｜ 李四 ｜
 * -------------
 * 我们可以把从source拉取数据的过程抽象成对这个table进行select的过程
 * 同时，我们也可以把sink想象成一个table，这个table不能select数据，只能insert数据，insert到这个table的数据
 * -------------
 * ｜id ｜name ｜
 * -------------
 * ｜1  ｜ 张三 ｜ -> 我们可以把往sink发送了一条数据的过程想象成往这个table插入了一条数据
 * -------------
 * 因此，我们就可以把最初source -> map -> sink的程序转换成一条sql语句：
 * insert into sink_table select func1(column1),func2(column2) from source_table
 * <p>
 * 可以看到这样非常大的简化了flink的程序。可以看到，flink sql是对flink程序的抽象。我们将flink程序修改为从一些source_table里select数据，通过一些function来对字段进行转换处理，最后往sink_table里insert程序。
 * <p>
 * 知道了这一点后，我们要说的细节了
 * 第一个细节是table字段的定义：
 * 由于使用table代替source或sink，因此table中的字段就要和source或sink中数据的字段一致。假如source中的数据有三个字段id,name,age，那么我们为这个source定义table时，也要有id,name,age这三个字段。
 * 假设往sink写入的字段有name,age,sex三个字段，那么table中也要有name,age,sex这三个字段，同时注意一点是，由于sink一般都是向数据库写入数据，因此也需要保证数据库中也有name,age,sex这三个字段。
 * <p>
 * 第二个细节是table数据的转换：
 * 现在我们定义好了table，我们下一个问题就是在select一个table时，如何把source里的数据转换成table中的数据，以及当往table里insert一条数据后，如何这行数据转换成要发给sink算子的数据。
 * 这个就涉及到在定义table时，with关键字里使用的format选项。上面我们说了如果source中的数据格式是json的，那么我们要在对应table中配置format=json，那么flinksql就会使用jackson对source中的数据
 * 进行解析，然后将json的字段赋值到table对应的字段中。而针对es sink的table，由于es中需要文档是json格式的，因此es sink的table的format也是json，flinksql会把插入table的数据使用jackson转换成json格式的数据。
 * 以下是es connector需要使用设置format=json的文献：
 * Elasticsearch stores document in a JSON string. So the data type mapping is between Flink data type and JSON data type. Flink uses built-in 'json' format for Elasticsearch connector.
 */
public class HelloWorldFlinkSql {

    public static void main(String[] args) throws Exception {
//        testFlinkSqlWithDataStream();
        testHive();
    }

    public static void testBase() throws Exception {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().build();

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);


        TableResult tableResult = streamTableEnvironment.executeSql("CREATE TABLE my_hello_world_123 (\n" +
                "  name string\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'hello-world',\n" +
                " 'properties.bootstrap.servers' = 'kafka:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'raw',\n" +
                " 'sink.parallelism' = '1'\n" +
                ")");


//        Table table = streamTableEnvironment.sqlQuery("select * from my_hello_world_123");

//        streamTableEnvironment.executeSql("insert into my_hello_world_123 values ('zhangsan123')");
//        streamTableEnvironment.executeSql("insert into my_hello_world_123 values ('lisi123')");
//        streamTableEnvironment.executeSql("insert into my_hello_world_123 values ('wangwu123')");
//        streamTableEnvironment.executeSql("insert into my_hello_world_123 values ('zhaoliu123')");

        Table table = streamTableEnvironment.sqlQuery("select * from my_hello_world_123");
        Table table1 = table.addColumns(Expressions.lit("test"));
        DataStream<RowData> rowDataDataStream = streamTableEnvironment.toAppendStream(table1, RowData.class);
        SingleOutputStreamOperator<String> stringStream = rowDataDataStream.map(rowData -> rowData.getString(0).toString()).returns(String.class);
        stringStream.print();
        streamExecutionEnvironment.execute();


//        TableResult helloWorld = table.executeInsert("hello_world");
//        System.out.println(helloWorld);
    }

    public static void testWithEs() {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().build();

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);

        streamTableEnvironment.executeSql("CREATE TABLE my_hello_world_kafka (\n" +
                "  id int,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  level int,\n" +
                "  birthday string\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'hello-world-json',\n" +
                " 'properties.bootstrap.servers' = 'kafka:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                ")");

//        streamTableEnvironment.executeSql("insert into my_hello_world_kafka values(1,'zhangsan',15,2,'2021-04-20T09:52:56.000+00:00')");
//        streamTableEnvironment.executeSql("insert into my_hello_world_kafka values(2,'lisi',13,3,'2021-04-21T13:23:50.000+00:00')");

        // 一个table的connector决定了它是否可以做source还是sink，像使用elasticsearch-7这个connect的table，它就只能做sink，因此只能对这个table做insert语句，不能select这个table
        // 对这个table进行select的话会报如下错误：Connector 'elasticsearch-7' can only be used as a sink. It cannot be used as a source.
        // 而使用connector为kafka的table，既可以是source，也可以是sink，因此既可以对这个table做insert语句，又可以对这个table做insert语句
        /*
            主键字段必须加上NOT ENFORCED的配置
            Flink doesn't support ENFORCED mode for PRIMARY KEY constaint. ENFORCED/NOT ENFORCED  controls if the constraint checks are performed on the incoming/outgoing data.
            Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode
         */
        streamTableEnvironment.executeSql("create table my_hello_world_elasticsearch(id int comment '编号',name string comment '名称',age int comment '年龄',level int comment '等级',birthday string,PRIMARY KEY (id) NOT ENFORCED)" +
                " with(  " +
                "'connector' = 'elasticsearch-7'," +
                "'hosts' = 'my-elasticsearch:9200'," +
                "'index' = 'student'," +
                "'format' = 'json'" +
                ")");

        streamTableEnvironment.executeSql("insert into my_hello_world_elasticsearch select id,name,age,level,birthday from my_hello_world_kafka");

//        TableResult queryResult = streamTableEnvironment.executeSql("select * from my_hello_world_kafka");
//        CloseableIterator<Row> collect = queryResult.collect();
//        while (collect.hasNext()) {
//            Row row = collect.next();
//            StringJoiner stringJoiner = new StringJoiner(",");
//            for (int i = 0; i < row.getArity(); i++) {
//                Object field = row.getField(i);
//                stringJoiner.add(Optional.ofNullable(field).map(Objects::toString).orElse("-"));
//            }
//            System.out.println(stringJoiner);
//        }
    }

    public static void testFlinkSqlWithDataStream() {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, environmentSettings);

        DataStreamSource<WaybillRouteLink> waybillRouteLinkDataStreamSource = streamExecutionEnvironment.addSource(new WaybillRouteLinkSource());
        Table waybillRouteLinkTable = streamTableEnvironment.fromDataStream(waybillRouteLinkDataStreamSource);

        streamTableEnvironment.executeSql("create table waybill_route_linke_fs(waybillCode string,packageCode string,staticDeliveryTime timestamp) " +
                "with('connector'='filesystem','path'='file:///my-files','format'='json','json.map-null-key.mode'='DROP')");

        streamTableEnvironment.executeSql("insert into waybill_route_linke_fs select waybillCode,packageCode,TO_TIMESTAMP(FROM_UNIXTIME(staticDeliveryTime)) from " + waybillRouteLinkTable);
    }

    public static void testHive() {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        streamExecutionEnvironment.setMaxParallelism(1);
        streamExecutionEnvironment.enableCheckpointing(TimeUnit.MINUTES.toMillis(5));

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment);
        HiveCatalog hiveCatalog = new HiveCatalog("myhive", "hello_world", "/my-repository/sql-cli/");
        tableEnvironment.registerCatalog("myhive", hiveCatalog);
        tableEnvironment.useCatalog("myhive");

        tableEnvironment.executeSql("CREATE TABLE IF NOT EXISTS kafka_source (\n" +
                "  id int,\n" +
                "  name string\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'hello_world',\n" +
                " 'properties.bootstrap.servers' = 'kafka-1:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'scan.startup.mode' = 'latest',\n" +
                " 'format' = 'json',\n" +
                " 'sink.parallelism' = '1'\n" +
                ")");

        tableEnvironment.executeSql("insert into hive_test/*+ options('sink.partition-commit.policy.kind'='metastore,success-file')*/ select id,name from kafka_source");
    }
}
