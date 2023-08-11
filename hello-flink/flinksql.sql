create table student(id int
                    ,name string
                    ,birthday timestamp(3)
                    ,age int
                    ,sex string
                    ,teacher_id int
                    -- 定义元数据字段的第一种方式：直接使用元数据对应的属性名
                    ,topic string metadata virtual
                    -- 定义元数据字段的第二种方式：使用自定义字段名，然后再使用metadata from '元数据属性名'定义要使用哪个元数据
                    ,event_partition int metadata from 'partition' virtual
                    -- 注意：对于read only的元数据字段，需要使用关键字virtual来描述，这样在插入数据时，flinksql就不会尝试使用该元数据字段插入到kafka中了
                    ,event_offset int metadata from 'offset' virtual)
                 with('connector'='kafka'
                    ,'properties.bootstrap.servers'='kafka-1:9092'
                    ,'topic'='hello_database_student'
                    ,'properties.group.id'='flinksql-group'
                    ,'properties.client.id'='flinksql-client'
                    ,'scan.startup.mode'='earliest-offset'
                    ,'key.format'='raw'
                    ,'key.fields'='id'
                    ,'value.format'='canal-json');

create table student(id int
                    ,name string
                    ,birthday timestamp(3)
                    ,age int
                    ,sex string
                    ,teacher_id int)
                 with('connector'='kafka'
                    ,'properties.bootstrap.servers'='kafka-1:9092'
                    ,'topic'='hello_database_student'
                    ,'properties.group.id'='flinksql-group'
                    ,'properties.client.id'='flinksql-client'
                    ,'scan.startup.mode'='earliest-offset'
                    ,'key.format'='raw'
                    ,'key.fields'='id'
                    ,'value.format'='canal-json');

create table student_summary_to_elasticsearch(
                age int primary key,
                name array<string>)
            with('connector'='elasticsearch-7'
                ,'hosts'='http://my-elasticsearch:9200'
                ,'format'='json'
                ,'index'='summary-student');

insert into student_summary_to_elasticsearch
        values(3,array['hello','world','test']);

select id,name,min(age) min_age,max(age) max_age from student group by id,name;

select * from student s1 join student s2 on s1.id=s2.id-1;

create table student(id int
                    ,name string
                    ,birthday timestamp(3)
                    ,age int
                    ,sex string
                    ,teacher_id int
                    ,proc_time as PROCTIME())
                 with('connector'='kafka'
                    ,'properties.bootstrap.servers'='kafka-1:9092'
                    ,'topic'='hello_database_student'
                    ,'properties.group.id'='flinksql-group'
                    ,'properties.client.id'='flinksql-client'
                    ,'scan.startup.mode'='earliest-offset'
                    ,'key.format'='raw'
                    ,'key.fields'='id'
                    ,'value.format'='canal-json');

sELECT window_start, window_end, SUM(id) FROM TABLE(
   TUMBLE(TABLE student, DESCRIPTOR(proc_time), INTERVAL '10' MINUTES))
   GROUP BY window_start, window_end;

create table hello_world_1(id string,
                        name string,
                        update_time timestamp(3),
                        watermark for update_time as update_time - interval '30' second)
                        with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092'
                            ,'topic'='hello_world_1'
                            ,'scan.startup.mode'='earliest-offset'
                            ,'key.format'='raw'
                            ,'key.fields'='id'
                            ,'value.format'='json');

create table hello_world_2(id string,
                        name string,
                        update_time timestamp(3),
                        watermark for update_time as update_time - interval '30' second)
                        with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092','topic'='hello_world_2','scan.startup.mode'='earliest-offset','key.format'='raw','key.fields'='id','value.format'='json');

insert into hello_world_1 values('3','lisi',TIMESTAMP '2021-12-08 07:00:00');
insert into hello_world_1 values('3','lisi1',TIMESTAMP '2021-12-08 07:10:00');
insert into hello_world_1 values('3','lisi2',TIMESTAMP '2021-12-08 08:30:00');
insert into hello_world_1 values('3','lisi3',TIMESTAMP '2021-12-08 08:40:00');
insert into hello_world_1 values('3','lisi4',TIMESTAMP '2021-12-08 08:50:00');
insert into hello_world_1 values('3','lisi5',TIMESTAMP '2021-12-08 08:55:00');
insert into hello_world_1 values('3','lisi6',TIMESTAMP '2021-12-08 08:56:00');

insert into hello_world_2 values('3','ls',TIMESTAMP '2021-12-08 07:30:00');
insert into hello_world_2 values('3','ls1',TIMESTAMP '2021-12-08 07:20:00');
insert into hello_world_2 values('3','lsx',TIMESTAMP '2021-12-08 07:40:00');
insert into hello_world_2 values('3','ls2',TIMESTAMP '2021-12-08 09:20:00');
insert into hello_world_2 values('3','ls3',TIMESTAMP '2021-12-08 09:00:00');
insert into hello_world_2 values('3','ls4',TIMESTAMP '2021-12-08 09:10:00');
insert into hello_world_2 values('2','zs77',TIMESTAMP '2021-12-08 06:30:00');

insert into hello_world_1 values('2','zhangsanxx',TIMESTAMP '2021-12-08 06:30:00');
insert into hello_world_1 values('2','zhangsanxxy',TIMESTAMP '2021-12-08 06:30:00');

insert into hello_world_2 values('2','zs88',TIMESTAMP '2021-12-08 06:30:00');

insert into hello_world_1 values('4','zhaowu',TIMESTAMP '2021-12-08 06:30:00');
insert into hello_world_2 values('4','zw',TIMESTAMP '2021-12-08 06:30:00');

select t1.id,t1.update_time,t1.name,t2.id,t2.name from (select * from hello_world_1 where id='2') t1 join hello_world_2 t2 on t1.id=t2.id and t1.update_time between t2.update_time - interval '1' hour and t2.update_time;

select window_start,window_end,id,listagg(name) from table(
   tumble(table hello_world, descriptor(proc_time), interval '10' seconds))
   group by window_start,window_end,id;

select id,window_start,window_end,count(name) name_count,listagg(name) concat_name from table(tumble(table hello_world,descriptor(proc_time),interval '10' seconds)) group by id,window_start,window_end;

insert into hello_world values ('6','hello'),('6','world');

select id,collect(name) from test1 group by id;

create view test123(id,name) as select id,name from hello_world;

select * from hello_world where upper(name) in ('HELLO','WORLD');

select id,array['a','b'] from hello_world;

select id,collect_set(name) from hello_world group by id;

create function collect_set as 'com.mzq.hello.flink.func.sql.CollectUniqueStringAggregate';

create table es_sink(id string primary key,name string,proc_time as PROCTIME()) with('connector'='elasticsearch-7','hosts'='http://my-elasticsearch:9200','index'='test_index','format'='json');

create table file_sink(id string,name string) with('connector'='filesystem','path'='/my-files/test123','format'='json');

select cast(id as string) id,listagg(name) from view123 group by id;

create view view456 as (select * from (values(1,'aa'),(1,'ba'),(1,'ca')) as t(id,name));

select cast(id as string) id,last_value(name) from view456 group by id;

select id,last_value(name)  from hello_world group by id;

select id,window_start,window_end,last_value(name)  from table(tumble(table hello_world,descriptor(proc_time),INTERVAL '20' SECONDS)) where id>3  group by id,window_start,window_end;
 insert into hello_world(id,name) values('1','heihei'),('1','lolo'),('3','pp'),('1','oo'),('1','qq'),('2','uu');
select id,LEAD(name)  from table(tumble(table hello_world,descriptor(proc_time),INTERVAL '20' SECONDS)) where id>3  group by id,window_start,window_end;

select * from (
    select id,name,proc_time,ROW_NUMBER() over (
                                                   partition by id
                                                   order by proc_time desc
                                               ) row_num  from hello_world
)
where row_num=1;

create table es_hello_world_window_aggregation(
    id string primary key,
    window_start timestamp(3),
    window_end timestamp(3),
    name_count string,
    concat_name string
) with(
    'connector'='elasticsearch-6',
    'hosts'='http://my-elasticsearch:9200',
    'index'='hello_world_window_aggregation',
    'format'='json'
);

select id,TUMBLE_START(proc_time, INTERVAL '10' seconds) as window_start,TUMBLE_END(proc_time, INTERVAL '10' seconds) as window_end,listagg(name) from hello_world
GROUP BY id,TUMBLE(proc_time, INTERVAL '10' seconds);


-- 注册function
create function length_words as 'com.mzq.hello.flink.sql.udf.table.LengthWords';

-- 直接调用table function，查看table function生成的表的数据
select * from LATERAL TABLE(length_words('hello world'));
-- 直接调用table function，查看table function生成的表的数据，然后使用as语句给生成的表的表名和字段名赋值
select my_lateral_table.new_words,my_lateral_table.new_length from LATERAL TABLE(length_words(123)) as my_lateral_table(new_words,new_length);
select * from LATERAL TABLE(length_words(date '2021-10-30'));

-- table function join
select * from (select * from hello_world where name is not null),lateral table(length_words(name));
-- 这种inner join的方式是将table function返回的内容直接贴到左表的列中，因为在调用table function时传了左表的字段，所以就知道把table function返回的数据贴在左表的哪一行上。但是如果table function没有返回数据，那么左表对应行的数据就查询不出来了
select t1.id,t1.name,t2.new_words,t2.new_length from (select * from hello_world where name is not null) t1,lateral table(length_words(name)) as t2(new_words,new_length);
-- 这种left outer join的方式与inner join方式几乎相同，只不过是如果table function没有返回数据，也可以把左表的数据查询出来，此时右表的字段值都为null
select t1.id,t1.name,t2.new_words,t2.new_length from (select * from hello_world) t1 left join lateral table(length_words(name)) as t2(new_words,new_length) on true;
select * from (
    select * from (values(1,'hello'),(2,'worlds')) as t(id,name)
) t1,lateral table(alias_search(t1.id)) t2;

create function redis_search as 'com.mzq.hello.flink.sql.udf.table.RedisSearch';
-- 利用table和table function join，完成根据左表的字段查询维表数据的目的
select * from (select * from hello_world where name is not null) t1 left join lateral table(redis_search(name)) as t2(origin_name,redis_key,redis_value) on true;

create function redis_query as 'com.mzq.hello.flink.sql.udf.scalar.RedisQuery';
-- scalar function使用
select id,name,redis_query(name) score from  hello_world;
select name,redis_query(name) from (select name from (values('t'),('a'),('b')) as t(name));

create function generate_rows as 'com.mzq.hello.flink.sql.udf.table.GenerateRows';
select * from table(generate_rows());
-- 设置global parameters
set pipeline.global-job-parameters=redis.url:'redis://my-redis:6379',my-param:hello_world;

CREATE TABLE orders (
 id INT primary key,
 name STRING,
 description STRING,
 order_time timestamp(3),
 update_time timestamp(3),
 watermark for update_time as update_time - interval '30' second
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'my-mysql',
 'port' = '3306',
 'username' = 'user_binlog',
 'password' = '123456',
 'database-name' = 'hello_database',
 'table-name' = 'orders'
);

CREATE TABLE shipment (
 id INT primary key,
 name STRING,
 ship_time timestamp(3),
 order_id int,
 update_time timestamp(3),
 watermark for update_time as update_time - interval '30' second
) WITH (
 'connector' = 'mysql-cdc',
 'hostname' = 'my-mysql',
 'port' = '3306',
 'username' = 'user_binlog',
 'password' = '123456',
 'database-name' = 'hello_database',
 'table-name' = 'shipment'
);

select window_start,window_end,max(name) from table(tumble(table orders,descriptor(update_time),INTERVAL '10' SECONDS)) group by window_start,window_end;
select id,max(name) from orders group by id;

select * from orders left join shipment on orders.id=shipment.order_id;

SELECT *
FROM orders o left join shipment s
on  o.id = s.order_id
AND o.order_time BETWEEN s.ship_time - INTERVAL '20' HOUR AND s.ship_time;

insert into print_sink
select id,max(o_name) from (
SELECT
    orders.id,orders.name o_name,shipment.name s_name
FROM orders
LEFT JOIN shipment FOR SYSTEM_TIME AS OF orders.update_time
ON orders.id = shipment.id)
group by id;

create table print_sink(id int,name string) with('connector'='print');

create table test_es(
                id int primary key,
                name1 string,
                name2 string)
            with('connector'='elasticsearch-7'
                ,'hosts'='http://my-elasticsearch:9200'
                ,'format'='json'
                ,'index'='test123');


SELECT *
FROM hello_world t1 , hello_world_1 t2
where  t1.id = cast((cast(t2.id as int)+1) as string)
AND t1.proc_time BETWEEN t2.proc_time - INTERVAL '1' hour AND t2.proc_time;

select t1.id,t1.name,t1.window_start,t1.window_end,t2.id,t2.name,t2.window_start,t2.window_end from (
    select * from table(tumble(table hello_world_1,descriptor(update_time),interval '1' minute))
) t1 left join (
    select * from table(tumble(table hello_world_2,descriptor(update_time),interval '1' minute))
) t2 on t1.id=t2.id and t1.window_start=t2.window_start and t1.window_end=t2.window_end;

insert into hello_world_2 values('1','zsx1',TIMESTAMP '2021-12-08 02:35:00');

select id,listagg(name) from table(tumble(table hello_world,descriptor(proc_time),interval '10' second)) group by id,window_start,window_end;


insert into hello_world_1 values('6','test1',TIMESTAMP '2021-12-07 06:30:00');
insert into hello_world_2 values('6','tt1',TIMESTAMP '2021-12-07 07:33:00');

select id,window_start,window_end from table(tumble(table hello_world_1,descriptor(update_time),interval '5' second)) group by id,window_start,window_end;



insert into hello_world_2 values('1','test1',TIMESTAMP '2021-12-11 02:30:01');
insert into hello_world_2 values('2','test1',TIMESTAMP '2021-12-10 23:31:01');
insert into hello_world_2 values('3','test1',TIMESTAMP '2021-12-10 23:32:01');
insert into hello_world_2 values('4','test1',TIMESTAMP '2021-12-11 01:33:01');
insert into hello_world_2 values('5','test1',TIMESTAMP '2021-12-10 23:34:01');
insert into hello_world_2 values('6','test1',TIMESTAMP '2021-12-10 23:35:01');
insert into hello_world_2 values('7','test1',TIMESTAMP '2021-12-10 23:35:01');
insert into hello_world_2 values('8','test1',TIMESTAMP '2021-12-11 03:35:01');

-- 演示sql
create table es_sink(
    id string primary key,
    name string,
    update_time timestamp(3)
)with('connector'='elasticsearch-7'
     ,'hosts'='http://my-elasticsearch:9200'
     ,'index'='hello_world'
     ,'format'='json');

insert into es_sink
select id,listagg(concat(name,'-helloworld')) concat_name,max(update_time)
from hello_world
group by id;

insert into hello_world values('16','world',TIMESTAMP '2021-12-09 02:20:20'),('16','world',TIMESTAMP '2021-12-09 02:10:20'),('16','world',TIMESTAMP '2021-12-09 02:30:20'),('16','world',TIMESTAMP '2021-12-09 01:33:55');
insert into hello_world values('16','world',TIMESTAMP '2021-12-01 11:23:50');
insert into hello_world values('16','world',TIMESTAMP '2021-12-12 05:25:31');
insert into hello_world values('16','world',TIMESTAMP '2021-12-13 03:12:00');
insert into hello_world values('16','world',TIMESTAMP '2021-12-08 23:35:20');
insert into hello_world values('16','world',TIMESTAMP '2021-12-08 23:35:30');
insert into hello_world values('16','world',TIMESTAMP '2021-12-08 08:35:30');
insert into hello_world values('16','world',TIMESTAMP '2021-12-07 22:11:25'),('16','world',TIMESTAMP '2021-12-06 11:25:38'),('16','world',TIMESTAMP '2021-12-07 23:19:06'),('16','world',TIMESTAMP '2021-12-01 06:12:31'),('16','world',TIMESTAMP '2021-12-13 09:15:22');
insert into hello_world values('6','world',TIMESTAMP '2021-12-07 22:11:25'),('7','world',TIMESTAMP '2021-12-06 11:25:38'),('8','world',TIMESTAMP '2021-12-07 23:19:06'),('9','world',TIMESTAMP '2021-12-01 06:12:31'),('10','world',TIMESTAMP '2021-12-13 09:15:22');
insert into hello_world values('11','world',TIMESTAMP '2021-12-07 22:11:25'),('12','world',TIMESTAMP '2021-12-06 11:25:38'),('13','world',TIMESTAMP '2021-12-07 23:19:06'),('19','world',TIMESTAMP '2021-12-01 06:12:31'),('20','world',TIMESTAMP '2021-12-13 09:15:22');
insert into hello_world values('4','world',TIMESTAMP '2021-12-07 22:11:25'),('5','world',TIMESTAMP '2021-12-06 11:25:38'),('6','world',TIMESTAMP '2021-12-07 23:19:06'),('7','world',TIMESTAMP '2021-12-01 06:12:31'),('8','world',TIMESTAMP '2021-12-13 09:15:22')
,('4','world',TIMESTAMP '2021-12-07 22:11:25'),('5','world',TIMESTAMP '2021-12-06 11:25:38'),('6','world',TIMESTAMP '2021-12-07 23:19:06'),('7','world',TIMESTAMP '2021-12-01 06:12:31'),('8','world',TIMESTAMP '2021-12-13 09:15:22')
,('4','world',TIMESTAMP '2021-12-07 22:11:25'),('5','world',TIMESTAMP '2021-12-06 11:25:38'),('6','world',TIMESTAMP '2021-12-07 23:19:06'),('7','world',TIMESTAMP '2021-12-01 06:12:31'),('8','world',TIMESTAMP '2021-12-13 09:15:22')
,('4','world',TIMESTAMP '2021-12-07 22:11:25'),('5','world',TIMESTAMP '2021-12-06 11:25:38'),('6','world',TIMESTAMP '2021-12-07 23:19:06'),('7','world',TIMESTAMP '2021-12-01 06:12:31'),('8','world',TIMESTAMP '2021-12-13 09:15:22')
,('4','world',TIMESTAMP '2021-12-07 22:11:25'),('5','world',TIMESTAMP '2021-12-06 11:25:38'),('6','world',TIMESTAMP '2021-12-07 23:19:06'),('7','world',TIMESTAMP '2021-12-01 06:12:31'),('8','world',TIMESTAMP '2021-12-13 09:15:22')
,('4','world',TIMESTAMP '2021-12-07 22:11:25'),('5','world',TIMESTAMP '2021-12-06 11:25:38'),('6','world',TIMESTAMP '2021-12-07 23:19:06'),('7','world',TIMESTAMP '2021-12-01 06:12:31'),('8','world',TIMESTAMP '2021-12-13 09:15:22');
insert into hello_world values('4','world',TIMESTAMP '2021-12-20 22:11:25'),('5','world',TIMESTAMP '2021-12-20 11:25:38');

select id,window_start,window_end,listagg(concat(name,'-helloworld')) concat_name
from table(tumble(table hello_world,descriptor(update_time),interval '5' second))
group by id,window_start,window_end;

select id,agg_test(name) concat_name
from hello_world
group by id;

create view test_view1(id,name,alias) as  select id,name,'unknown' alias from (values(1,'hello')) as t(id,name);
create view test_view2 as  select id,'unknown' name,alias from (values(1,'hh'),(2,'ww')) as t(id,alias);

select id,last_value(NULLIF(name,'unknown')),last_value(NULLIF(alias,'unknown')) from (
select * from test_view1 t1
union
select * from test_view2 t2
)
group by id;

create function test as 'com.mzq.hello.flink.sql.udf.scalar.AliasQuery';
select id,name,test(1) as tt from hello_world;
select id,name,update_time,UNIX_TIMESTAMP('2021-12-15 09:32:55')*1000 from hello_world;
select UNIX_TIMESTAMP('2021-12-15 09:32:55'),TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP('2021-12-15 09:32:55'),0);

create function show_time as 'com.mzq.hello.flink.sql.udf.scalar.ShowTime';

select show_time(to_timestamp('2021-10-31 10:32:30'));
select UNIX_TIMESTAMP('2021-10-31 10:32:30');
select show_time(current_timestamp);
select FROM_UNIXTIME(UNIX_TIMESTAMP()),UNIX_TIMESTAMP();

select * from (values(1,'hello'))
union
select * from (values(1,cast(null as string)));

create table hello_world_1(id string,
                        name string,
                        update_time timestamp(3),
                        watermark for update_time as update_time + interval '30' second)
                        with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092'
                            ,'topic'='hello_world_1'
                            ,'scan.startup.mode'='earliest-offset'
                            ,'key.format'='raw'
                            ,'key.fields'='id'
                            ,'value.format'='json');
insert into hello_world values('10','world',TIMESTAMP '2021-11-08 11:21:37');

select id,name,update_time,min(cast(id as int)) over (
                                                   partition by name
                                                   order by update_time
                                               ) min_id  from hello_world;

select cast('a123' as int) is null;

select case when 1>0 then case when 2>3 then 'a' else 'b' end end;

create table hello_world(doc)
                        with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092'
                            ,'topic'='hello_world'
                            ,'scan.startup.mode'='earliest-offset'
                            ,'key.format'='raw'
                            ,'key.fields'='doc'
                            ,'value.format'='raw');

create function test123 as 'com.mzq.hello.flink.sql.udf.table.Test123';

select * from hello_world join lateral table(test123(doc)) on true;

select cast(array['1','2','3'] as array<int>);

select * from hello_world_1 where id='15'
union all
select * from hello_world_2 where id='16';

select id,last_value(name) from hello_world_2 group by id,tumble(update_time,interval '0.3' second);

select id,max(test) is true
from(
    select id,test from (values(1,true),(1,false)) as t(id,test)
)
group by id;

 select id,name,lag(update_time) over(partition by id order by update_time) lead_time from hello_world;

create table hello_world_1(id string,
                        name string,
                        update_time as proctime())
                        with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092'
                            ,'topic'='hello_world_1'
                            ,'scan.startup.mode'='earliest-offset'
                            ,'key.format'='raw'
                            ,'key.fields'='id'
                            ,'value.format'='json');
create table hello_world_2(id string,
                        name string,
                        update_time as proctime())
                        with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092'
                            ,'topic'='hello_world_2'
                            ,'scan.startup.mode'='earliest-offset'
                            ,'key.format'='raw'
                            ,'key.fields'='id'
                            ,'value.format'='json');

insert into hello_world_1 values('16','hello',TIMESTAMP '2021-12-08 23:35:30'),('16','world',TIMESTAMP '2021-12-08 08:35:30');
insert into hello_world_2 values('16','test',TIMESTAMP '2021-12-08 23:35:30');
insert into hello_world_2 values('16','tt',TIMESTAMP '2021-12-08 23:35:30');
select t1.id,t1.name,t2.name from hello_world_1 t1 left join hello_world_2 t2 on t1.id=t2.id;

CREATE TABLE mysql_table (
  id string,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://my-mysql:3306/my_database',
   'table-name' = 'test_table',
   'username' = 'root',
   'password' = '123456'
);

select
    t1.id,
    t1.name,
    mysql_table.name alias
from hello_world_1 t1 left join mysql_table for SYSTEM_TIME as of t1.update_time
on t1.id=mysql_table.id;

select
    t1.id,
    t1.name,
    t2.name alias,
    t1.update_time ltime,
    t2.update_time rtime
from hello_world_1 t1
left join hello_world_2 t2
on t1.id=t2.id
and t2.update_time between t1.update_time - interval '20' second and t1.update_time+ interval '20' second;

insert into hello_world_2 values('222','dada'),('222','mama'),('222','fafa');
insert into hello_world_1 values('222','yaya'),('222','rara');
insert into hello_world_2 values('222','chacha');
insert into hello_world_1 values('222','haha'),('222','lala');

-- lookup join start
create table jdbc_source(
                id string,
                name string,
                version int,
                alias string,
                row_time as proctime())
            with('connector'='jdbc'
                ,'url'='jdbc:mysql://my-mysql:3306/my_database'
                ,'table-name'='student_alias_version'
                ,'username'='root'
                ,'password'='123456'
                ,'lookup.cache.max-rows'='1'
                ,'lookup.cache.ttl'='30 second');

create table hello_world_1(id string,
                        name string,
                        update_time as proctime())
                        with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092'
                            ,'topic'='hello_world_1'
                            ,'scan.startup.mode'='earliest-offset'
                            ,'key.format'='raw'
                            ,'key.fields'='id'
                            ,'value.format'='json');

insert into hello_world_1 values('16','haha'),('13','nana'),('16','jiojio');
insert into jdbc_source values('16','haha',1,'hh1'),('16','haha',2,'hh2'),('13','nana',1,'nn'),('16','jiojio',1,'jj');
select
    t1.id,
    t1.name,
    t2.alias
from hello_world_1 t1 left join jdbc_source for system_time as of t1.update_time as t2
on t1.id=t2.id
and t1.name=t2.name
and t2.version=2;

with test_table as(
    select id,age from
    (values(1,18),(1,cast(null as int))) as t(id,age)
)
select
    id,last_value(age)
from test_table
group by id;

create table mysql_hello(
    id int primary key,
    name string,
    update_time timestamp(3),
    watermark for update_time as update_time - interval '5' second
)with(
    'connector'='jdbc',
    'url'='jdbc:mysql://my-mysql:3306/my_database',
    'table-name'='hello_world',
    'username'='root',
    'password'='123456'
);

select
    window_start,window_end,count(name) count_num
from table(tumble(table mysql_hello,descriptor(update_time),interval '5' minute))
group by window_start,window_end;

CREATE CATALOG myhive WITH ('type'='hive','default-database'='default','hive-conf-dir'='/flink-libs');
set table.sql-dialect=hive;

create table kafka_test(id int,
                        name string,
                        update_time as proctime())
                        with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092'
                            ,'topic'='kafka_test'
                            ,'scan.startup.mode'='earliest-offset'
                            ,'key.format'='raw'
                            ,'key.fields'='id'
                            ,'value.format'='json');

insert into kafka_test values(1,'zhangsan'),(1,'zhangsan');

select
    id,count(*)
from(
    select name,last_value(id) id
    from kafka_test
    group by name
)
group by  id;

insert into kafka_test(name) values('kaikai');
insert into kafka_test values(888,'kaikai');

-- 把hive表作为streaming数据源和kafka数据源进行join，此时hive表中数据有新增的话，也会从kafka数据源中找到可以匹配的数据
select
    t1.id,t1.name,t2.name alias
from kafka_test t1
-- 如果想把hive表作为streaming数据源使用，需要使用options，覆盖默认配置，默认streaming-source.enable=false
left join hello_hive /*+ OPTIONS('streaming-source.enable'='true','streaming-source.partition.include'='all','streaming-source.monitor-interval'='5 s','streaming-source.partition-order'='partition-name') */ as t2
on t2.id=t1.id;

-- 把hive表作为维表来和kafka数据源进行join，此时hive表中有
select
    t1.id,
    t1.name,
    t2.name alias
from kafka_test t1
-- 此时把hive作为维表使用的话，就不需要为hello_hive表设置streaming-source.enable=false，因为默认就是。此时我们可以设置cache ttl，来增加内存缓存设置
left join hello_hive_multi_partition /*+ options('lookup.join.cache.ttl'='10 min') */ for system_time as of t1.update_time as t2
on t2.id=t1.id;

--利用flinksql的options功能对hive的table进行修改。
--streaming-source.enable=true，代表使用流的方式读取hive表，即先读全表，然后隔一段时间获取hive表中新增的数据并发送至下游。如果该属性为false，那么hive source算子在读取完hive table的全部数据后，则变成complete状态，不会再从hive读取新的数据了。
--streaming-source.partition.include=all，代表在读取hive的分区表时，需要读取全部分区的数据。该属性的可选值为：all、latest。如果为latest，那么hive source会按照streaming-source.partition-order的配置决定哪个分区是最新的，并只读取最新分区的数据。
--         注意：streaming-source.partition.include=latest只适用于把该hive表当作维表来查询时使用。在使用hive表进行维表join时，如果streaming-source.partition.include=latest，则代表从最新的分区获取数据。如果为all，则代表从整个table中获取数据。
--streaming-source.monitor-interval=5 s，代表在使用stream方式读取hive数据时，定时拉取数据的频率。
--streaming-source.partition-order=partition-name，代表hive source如何决定哪个分区是最新的。当使用stream方式读取hive数据时，在读取完全表数据后，只会定时读取最新分区的数据，该属性则代表了如何判断哪个分区是最新的分区。
--         streaming-source.partition-order的选项为：create-time、partition-time、parition name，以下是他们比较的内容：
--         create-time: compares partition/file creation time, this is not the partition create time in Hive metaStore,but the folder/file modification time in filesystem
--         partition-time: compares the time extracted from partition name
--         partition-name: compares partition name's alphabetical order
--         以streaming-source.partition-order=partition-name举例：如果分区字段是一个数值型的字段age，那么在全表扫描后，如果已经读取了age=30分区的数据，那么此时再往该表写age=20的数据，hive source是不会把它发送到下游的，因为它认为该分区的数据是老的了。而如果往该表写age=40分区的数据，那么hive source则会从获取该分区的数据，并发送到下游，因为该分区在排序后认为是新的分区。
--         该属性的默认值是create-time，并且如果读取的不是分区表的话，则只能使用create_time
-- 以下是使用stream的方式读取hive分区表的方式
select * from hello_hive
/*+ OPTIONS('streaming-source.enable'='true','streaming-source.partition.include'='all','streaming-source.monitor-interval'='5 s','streaming-source.partition-order'='partition-name') */;

-- 以下是使用stream的方式读取hive非分区表的方式
select * from hello_hive_non_partitioned
/*+ OPTIONS('streaming-source.enable'='true','streaming-source.partition.include'='alll','streaming-source.monitor-interval'='5 s','streaming-source.partition-order'='create-time') */;

-- 使用flinksql向分区表写入数据，需要为分区表配置sink.partition-commit.policy.kind属性，可选值为metastore或success-file，可多选，多个用逗号分割。
-- 该属性就是flink告诉hive本次需要向该分区写入的数据写完了。如果配置为metastore，那么flink会向hive metastore连接的数据库的PARTITIONS表里写入分区信息，如果配置为success-file，则会在向该分区写好数据文件后，产生一个_SUCCESS文件，代表这次写数据写完了。
insert into  hello_hive /*+ options('sink.partition-commit.policy.kind'='metastore,success-file') */ values(1,'jaja',20);

-- 如果写入的是非分区表，则不需要对hive table增加配置
insert into hello_hive_non_partitioned values(1,'yoyo');

-- flinksql可以把从hive中读取到的table当作bounded scan table source、unbounded scan table source、lookup table source、以及sink
insert into hello_hive /*+ options('sink.partition-commit.policy.kind'='metastore,success-file') */ select id,name,12 from kafka_test;

-- hive source在流方式读取数据时，采用的是全量查询 + 增量查询的方式监听hive表的数据。在全量查询阶段完成后，开启增量查询阶段。在增量查询阶段，hive source总是会根据分区字段的值来读取最新分区的数据，下面说的就是当他以【字典序】方式对分区字段值进行新旧判断时产生的问题。
-- 如果hive表中有多个分区字段的话，那么在使用流读取时会造成的问题是，flink是将所有分区字段的值以字符串的形式拼起来，然后以【字典序】的方式比较分区哪个是最新的。因此，对于以下两个分区：'2022-03-02 11'和'2022-03-02 9'，
-- 在以字典序比较时，前11个字符是相同的，在比较到第12个字符时，'2022-03-02 11'的第12个字符是1，'2022-03-02 9'的第12个字符是9，由于9比1大，flink就会认为分区'2022-03-02 9'，比分区'2022-03-02 11'大，
-- 导致新产生的分区'2022-03-02 11'中的数据，hive source就不会再读该数据并往后发了。这就是字典序导致的坑，实际情况下'2022-03-02 11'肯定比'2022-03-02 9'大，'2022-03-02 11'分区来的数据还是要发送到hive source的下游的。
select * from hello_hive_multi_partition /*+ options('streaming-source.enable'='true','streaming-source.partition.include'='all','streaming-source.monitor-interval'='5 s','streaming-source.partition-order'='partition-name')*/;

-- 在hive source以流方式读取数据时，采用的是全量查询 + 增量查询的方式监听hive表的数据。在全量查询时，默认情况下hive source会从hive中读取数据，此时会读取该hive表的所有分区的数据。如果该表数据量过大，并且我们只需要处理指定分区之后的数据，
-- 我们可以使用streaming-source.consume-start-offset属性来控制在全量查询时，从哪个分区开始读取数据，避免全表查询。
select * from hive_test /*+ options('streaming-source.enable'='true','streaming-source.partition.include'='all','streaming-source.monitor-interval'='5 s','streaming-source.partition-order'='partition-time','streaming-source.consume-start-offset'='2022-03-01') */;

-- 尽管hive_table在作为source以流方式读取数据时，仅读取最新分区的数据。但使用hive_table作为sink时，可以向任意历史分区写入数据，没有分区新旧的限制
insert into hello_hive_multi_partition /*+ options('sink.partition-commit.policy.kind'='metastore,success-file') */ values(11,'test',date '2011-03-02',912),(12,'test_1',date '2011-03-01',100),(11,'test_2',date '2011-03-02',111);

-- 可以使用sink.parallelism字段指定写hive表的算子的并发度
-- 1.写入hdfs算子的并发度：
--   sink.parallelism：Parallelism of writing files into external file system. The value should greater than zero otherwise exception will be thrown.
-- 2.分区提交trigger：控制分区何时提交。
--   sink.partition-commit.trigger：Trigger type for partition commit: 'process-time': based on the time of the machine, it neither requires partition time extraction nor watermark generation. Commit partition once the 'current system time' passes 'partition creation system time' plus 'delay'. 'partition-time': based on the time that extracted from partition values, it requires watermark generation. Commit partition once the 'watermark' passes 'time extracted from partition values' plus 'delay'.
--   sink.partition-commit.delay：The partition will not commit until the delay time. If it is a daily partition, should be '1 d', if it is a hourly partition, should be '1 h'.
-- 3.分区提交策略：分区提交就是在向指定分区写完数据后，会在metastore的partitions表中增加一条该分区的数据，或在该分区的文件夹中增加一个_SUCCESS文件，代表该分区的数据写完了。写metastore或文件，就是分区的提交策略。
--   sink.partition-commit.policy.kind：Policy to commit a partition is to notify the downstream application that the partition has finished writing, the partition is ready to be read. metastore: add partition to metastore. Only hive table supports metastore policy, file system manages partitions through directory structure. success-file: add '_success' file to directory. Both can be configured at the same time: 'metastore,success-file'. custom: use policy class to create a commit policy. Support to configure multiple policies: 'metastore,success-file'.
-- 注意：
--   StreamingFileWriter总会在整个任务checkpoint完成(notifyCheckpointComplete方法被调用时)时，将需要写入的数据写入到hdfs。然后将可以提交的分区作为一个数据，发送给下游PartitionCommitter算子，告知PartitionCommitter该进行分区提交了。PartitionCommitter会根据配置向metastore写数据或向hdfs中该分区文件夹写_SUCCESS文件。
--   分区是否可以提交是由StreamingFileWriter来判断的，它在proctime场景下的判断公式为：commitDelay == 0 || currentProcTime > predicateContext.createProcTime() + commitDelay。返回true则可以提交，否则不可以提交。其中predicateContext.createProcTime()为分区的创建时间。
--   通过这个公式可以发现，如果sink.partition-commit.delay='0 s'，那么分区总是可以提交的。
--   但是如果设置了sink.partition-commit.delay='5 s'，需要分区创建时间 + 5s < 当前时间才认为该分区是可以提交的。因此，本次checkpoint完成时，是不会提交分区的。
insert into hello_hive_multi_partition /*+ options('sink.partition-commit.policy.kind'='metastore,success-file','sink.parallelism'='3','sink.rolling-policy.file-size'='100b','sink.partition-commit.trigger'='process-time','sink.partition-commit.delay'='5 s') */
-- 我们可以使用catalog.database.table来访问其他catalog中的指定database的表，也就是在不切换catalog时，访问其他catalog下的database的表
select id,name,date '2022-03-18',18 from kafka_test;

set table.exec.hive.infer-source-parallelism=true;
set table.exec.hive.infer-source-parallelism.max=3;
set table.exec.resource.default-parallelism=2;
-- 在以批方式读取hive表数据时（'streaming-source.enable'='false'）：
--   可以通过配置任务的参数table.exec.hive.infer-source-parallelism=true来让flink根据hive中文件的多少来推算source需要多少个并发度。例如当hive_table中有6个文件时，那么如果启动该参数，则flink推断hive source的并发度是6
--   同时可以通过任务参数table.exec.hive.infer-source-parallelism.max来配置自动推断算出来的并发度最大是多少，避免超过flink集群的slot数。
--   如果table.exec.hive.infer-source-parallelism=false，那么flink会使用任务配置中的table.exec.resource.default-parallelism配置来配置hive source算子的并发度。
-- 在以流方式读取hive表数据时（'streaming-source.enable'='true'）：
--   table.exec.hive.infer-source-parallelism是不起作用的，flink只以table.exec.resource.default-parallelism配置来为hive source算子配置并发度
select *,row_number() over(partition by id order by row_time) from hello_hive_multi_partition
/*+ options('streaming-source.enable'='false') */;

set execution.checkpointing.interval='5 s';

with hello_hive_multi_partition_test as (
    select *,proctime() pt
    from hello_hive_multi_partition
)
select
    *
from(
    select *,row_number() over(partition by id order by pt) row_num from hello_hive_multi_partition_test
)
where
    row_num=1;

-- 'streaming-source.partition-order'='partition-name'
select
    *
from waybill_route_link
/*+ options('streaming-source.enable'='true','streaming-source.monitor-interval'='10 s','streaming-source.partition-order'='partition-name','streaming-source.consume-start-offset'='dp=HISTORY') */;

-- 'streaming-source.partition-order'='partition-time'
select
    *
from package_state
/*+ options('streaming-source.enable'='true','streaming-source.monitor-interval'='10 s','streaming-source.partition-order'='partition-time','partition.time-extractor.kind'='default','partition.time-extractor.timestamp-pattern'='$year-$month-$day','streaming-source.consume-start-offset'='2022-03-15')*/;

-- 'streaming-source.partition-order'='create-time'
select
    *
from package_state
/*+ options('streaming-source.enable'='true','streaming-source.monitor-interval'='10 s','streaming-source.partition-order'='create-time','streaming-source.consume-start-offset'='2022-03-14')*/;


select w,a,row_time,last_value(a) over(partition by w order by row_time) from(select w,a,proctime() row_time from (values('JDA',true),('JDA',false),('JDA',cast(null as boolean))) as t1(w,a));
select
    w,last_value(coalesce(a,b))
from(
        select w,a,row_time,last_value(a) over(partition by w order by row_time) b from(select w,a,proctime() row_time from (values('JDA',cast(null as boolean)),('JDA',false),('JDA',cast(null as boolean))) as t1(w,a))
    )
group by w,tumble(row_time,interval '0.1' second);

create table starflow_waybill(waybill_code string
                             ,order_id string
                             ,row_time as proctime() )
    with('connector'='kafka'
        ,'properties.bootstrap.servers'='kafka-1:9092'
        ,'topic'='starflow_waybill'
        ,'properties.group.id'='flinksql-group'
        ,'properties.client.id'='flinksql-client'
        ,'scan.startup.mode'='earliest-offset'
        ,'key.format'='raw'
        ,'key.fields'='waybill_code'
        ,'value.format'='json');

select * from starflow_waybill;

insert into starflow_waybill values('JD1',cast(null as string)),('JD1','OD1'),('JD1',cast(null as string))
insert into starflow_waybill values('JD1',cast(null as string));
insert into starflow_waybill values('JD5','OD5');
insert into starflow_waybill values('JD5',cast(null as string)),('JD5',cast(null as string));
insert into starflow_waybill values('JD5','OD6');
insert into starflow_waybill values('JD8',cast(null as string));
insert into starflow_waybill values('JD8','OD8');
insert into starflow_waybill values('JD8',cast(null as string));
insert into starflow_waybill values('JD9','1');

select waybill_code,order_id,last_value(order_id) over(partition by waybill_code order by row_time) history_order_id from starflow_waybill;

select waybill_code,last_value(coalesce(order_id,history_order_id)) from (
    select waybill_code,order_id,last_value(order_id) over(partition by waybill_code order by row_time) history_order_id,row_time from starflow_waybill
) group by waybill_code,tumble(row_time,interval '80' second);

select waybill_code,last_value(coalesce(order_id,history_order_id)) from (
    select waybill_code,order_id,last_value(order_id) over(partition by waybill_code order by row_time) history_order_id,row_time from starflow_waybill
) group by waybill_code,tumble(row_time,interval '80' second);


CREATE TABLE IF NOT EXISTS kafka_source (
                                            id int,
                                            name string
) WITH (
      'connector' = 'kafka',
      'topic' = 'hello_world',
      'properties.bootstrap.servers' = 'kafka-1:9092',
      'properties.group.id' = 'testGroup',
      'properties.auto.offset.reset' = 'earliest',
      'format' = 'json',
      'sink.parallelism' = '1'
      );


select id,name,count(*) cnt from kafka_source/*+options('properties.auto.offset.reset'='earliest')*/ group by grouping sets((id,name),(name))

CREATE TABLE hTable (
                        rowkey string,
                        info ROW<name string,level string>,
                        PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
      'connector' = 'hbase-1.4',
      'table-name' = 'dept',
      'zookeeper.quorum' = 'zookeeper:2181'
      );

set execution.runtime-mode=batch;
select
    waybill_code,
    route_site,
    lead(route_site) over(partition by waybill_code) downstream,
    lag(route_site) over(partition by waybill_code) upstream
from(
    select
        t.*,
        route_table.route_site
    from waybill_route_link t
    left join lateral table(explode(route)) as route_table(route_site) on true
);

create function explode as 'com.mzq.hello.flink.sql.udf.table.Explode';
select a from table(explode('1,2,3')) as t(a);

select
    t.*,
    route_site
from waybill_route_link t left join lateral table(explode(route)) as route_table(route_site) on true;

/*
    gropuing sets用于多聚合维度查询，例如grouping sets((dept_name),(sex),(dept_name,sex),())是按以下聚合多次维度
    1.按dept_name聚合，结果如下
        hr,null,10
        dev,null,20
    2.按sex聚合，结果如下
        null,male,11
        null,female,13
    3.按dept_namem,sex聚合，结果如下
        hr,male,3
        hr,female,7
        dev,male,8
        dev,female,13
    4.()代表按全表聚合，结果如下
        null,null,30

    grouping_id用于获取当前数据是按什么维度聚合的。grouping_id(dept_name,sex)：
    1.当按照dept_name聚合，dept_name字段有值，sex字段没有值，那么dept_name，sex就变为了二进制的01（有值的字段为0，没值的字段为1），01转换为十进制则是1。所以按dept_name聚合时，grouping_id(dept_name,sex)的值为1
        hr,null,10,1
        dev,null,20,1
    2.当按照sex聚合，dept_name字段没有值，sex字段有值，那么dept_name，sex就变为了二进制的10，转换为十进制是2。所以按sex聚合时，grouping_id(dept_name,sex)的值为2
        null,male,11,2
        null,female,13,2
    3.当按照dept_name,sex聚合时，dept_name和sex字段都有值，那么dept_name，sex就变为了二进制的00，转换为十进制是0。所以按dept_name、sex聚合时，grouping_id(dept_name,sex)的值为0
        hr,male,3,0
        hr,female,7,0
        dev,male,8,0
        dev,female,13,0
    4.当按照()聚合时，也就是全表聚合，dept_name和sex字段都没有值，那么dept_name，sex就变为了二进制的11，转换为十进制是3。所以按全表聚合时，grouping_id(dept_name,sex)的值为3
        null,null,30，3

    多维聚合的好处：
    1.仅用一次计算，可以算出多维度的聚合结果
    2.结合grouping_id()，我们可以轻松从多维度聚合结果中选取出我们需要的一个或多个维度的聚合结果的数据
*/
select
    dept_name,
    sex,
    count(*) cnt,
    grouping_id(dept_name,sex) gp_id
from employee
group by grouping sets((dept_name),(sex),(dept_name,sex),())
order by grouping_id(dept_name,sex);

-- rollup(dept_name,sex)是grouping sets的语法糖，提供了递进式的聚合维度。rollup(dept_name,sex)相当于grouping sets((dept_name,sex),(dept_name),())
select
    dept_name,
    sex,
    count(*) cnt,
    grouping_id(dept_name,sex) gp_id
from employee
group by rollup(dept_name,sex)
order by grouping_id(dept_name,sex);

-- cube也是grouping_sets的语法糖，相比rollup提供了更多的聚合维度。cube(dept_name,sex)相当于grouping sets((dept_name,sex),(dept_name),(sex),())
select
    dept_name,
    sex,
    count(*) cnt,
    grouping_id(dept_name,sex) gp_id
from employee
group by cube(dept_name,sex)
order by grouping_id(dept_name,sex);

select DATE_FORMAT(CEIL(CURRENT_TIMESTAMP TO HOUR),'yyyy-MM-dd HH:mm');
select substr(cast(CEIL(time '02:30:14' TO HOUR) as string),1,5);

select lpad(cast(hour(current_time) as string),2,'0')||lpad(cast((minute(current_time)/20)*20 as string),2,'0');
select (((minute(current_time)/15)+1)*15) -1

set execution.runtime-mode=batch;
set table.exec.hive.infer-source-parallelism=true;
set table.exec.hive.infer-source-parallelism.max=2;
set table.exec.resource.default-parallelism=1;
insert overwrite waybill_route_link/*+ options('sink.parallelism'='1') */ select * from waybill_route_link order by waybill_code,operate_type;

select hour(TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP('2022-09-01 12:32:33', 'yyyy-MM-dd HH:mm:ss'),0)) am_flag;

create table hbase_dept(
    rowkey string,
    info row<name string,level string,tel string>
)with(
    'connector'='hbase-1.4',
    'zookeeper.quorum'='zookeeper:2181',
    'table-name'='dept'
);

create table kafka_test(
    contents string
)with(
    'connector'='kafka',
    'properties.bootstrap.servers'='kafka-1:9092',
    'properties.client.id'='my-cli',
    'properties.group.id'='my-group',
    'properties.auto.offset.reset'='earliest',
    'topic'='hello_world',
    'format'='raw'
);

insert into hbase_dept values('1001',row('flink dev','l4',cast(null as string))),('1002',row('flink test','l5','137****3172'));

create table kafka_test(
         contents string
 )with(
                'connector'='kafka',
                'properties.bootstrap.servers'='kafka-1:9092',
                'properties.client.id'='my-cli',
                'properties.group.id'='my-group',
                'properties.auto.offset.reset'='earliest',
                'topic'='hello_world',
                'format'='raw'
            );

select
    t.contents,h.info
from (
     select *,proctime() row_time from kafka_test
 ) t
left join hbase_dept for system_time  as of t.row_time as h
on t.contents=h.rowkey;


create table hbase_employee(
    rowkey string,
    base row<emp_no string,emp_name string,sex string>,
    dept row<dept_no string,dept_name string>,
    info row<create_dt string,salary string>
)
with(
    'connector'='hbase-1.4',
    'zookeeper.quorum'='zookeeper:2181',
    'table-name'='employee'
);

create table hbase_employee_usage(
    rowkey string,
    info row<emp_no string,emp_name string,sex string,dept_no string,dept_name string,create_dt string,salary string>
)
with(
    'connector'='hbase-1.4',
    'zookeeper.quorum'='zookeeper:2181',
    'table-name'='employee_usage'
    );

set execution.runtime-mode=batch;

CREATE CATALOG myhive WITH ('type'='hive','default-database'='default','hive-conf-dir'='/my-repository');
use catalog myhive;

insert into hbase_employee
select
    emp_no,
    row(emp_no,emp_name,sex) base,
    row(dept_no,dept_name) dept,
    row(create_dt,salary_str) info
from (
    select *,cast(salary as string) salary_str
    from employee
);

insert into hbase_employee_usage
select
    emp_no,
    row(emp_no,emp_name,sex) base,
    row(dept_no,dept_name) dept,
    row(create_dt,salary_str) info
from (
    select *,cast(salary as string) salary_str
    from employee
);

insert into kafka_sink_employee_summary
select dept_no,dept_name,count(emp_no) cnt
from (
     select base.emp_no,dept.dept_no,dept.dept_name
     from hbase_employee
)
group by dept_no,dept_name;

select dept_no, dept_name,sum(salary) salary_sum, avg(salary) salary_avg, count(emp_no) cnt
from (select base.emp_no, dept.dept_no, dept.dept_name,cast(info.salary as int) salary
      from hbase_employee)
group by dept_no, dept_name;

select *
from (select *, row_number() over(partition by dept_name order by create_dt) rn
      from (select base.emp_no, dept.dept_no, dept.dept_name, info.create_dt
            from hbase_employee))
where rn<=3;

select
    rowkey,
    base.emp_no,
    base.emp_name,
    base.sex,
    dept.dept_no,
    dept.dept_name,
    info.create_dt,
    info.salary,
    avg(cast(salary as int)) over(partition by dept_name)
from hbase_employee
where
    info.salary between '2000' and '3000'
    and base.sex = 'female'
limit 30;

select count(*) cnt
from hbase_employee;

create table kafka_sink_employee_summary(
    dept_no string,
    dept_name string,
    cnt bigint,
    primary key (dept_no,dept_name) not enforced
)with(
    'connector'='upsert-kafka',
    'properties.bootstrap.servers'='kafka-1:9092',
    'topic'='employee_summary',
    'key.format'='json',
    'value.format'='json'
);

create function hash_code as 'com.mzq.hello.flink.sql.udf.scalar.HashCodeScalar';
select lpad(cast(hash_code('dept_1') % 10 as string),2,'0');

create table hbase_emp_usage(
                                     rowkey string,
                                     info row<emp_no string,emp_name string,sex string,dept_no string,dept_name string,create_dt string,salary string>
)
    with(
        'connector'='hbase-1.4',
        'zookeeper.quorum'='zookeeper:2181',
        'table-name'='emp_usage'
        );

insert into hbase_emp_usage
select
    --使用bucket编码(根据分区键计算出来)+分区键+主键生成rowkey
    concat(lpad(cast(abs(hash_code(dept_no)) % 10 as string),2,'0'),'_',dept_no,'_',emp_no) rowkey,
    row(emp_no,emp_name,sex,dept_no,dept_name,create_dt,salary_str) info
from (
         select *,cast(salary as string) salary_str
         from employee
     );

select *
from hbase_emp_usage
where
    -- 只要知道分区键和主键，就可以反向计算出该数据的rowkey，进行hbase精准查询
    rowkey = concat(lpad(cast(abs(hash_code('dept_14'))%10 as string), 2, '0'), '_', 'dept_14', '_', 'emp_15798882');

create 'emp_usage','info',SPLITS=>['00|','01|','02|','03|','04|','05|','06|','07|','08|']

create function explode as 'com.mzq.hello.flink.sql.udf.table.Explode';
select
    t1.w,
    t2.sw
from (select *
      from (values ('hello,world'), ('hello,laozhang')) as t(w)) t1,
     lateral table(explode(w)) as t2(sw);


select cast(FLOOR((current_date - interval '1' year) + interval '1' month to month) as string) st,cast(ceil((current_date - interval '1' year) + interval '1' month to month) as string) ed;

select if(json_value('','$.b') <> '',true,false);

CREATE CATALOG my_hudi WITH ('type'='hudi','mode'='dfs','catalog.path'='/my-hudi/hudi_catalog','default-database'='dev_db');
use catalog my_hudi;
use dev_db;

CREATE TABLE hudi_hello_world(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    ts bigint,
    dt string,
    primary key (waybillCode) not enforced
)
    PARTITIONED BY (`dt`)
WITH (
  'connector' = 'hudi',
 -- 如果已经给出了hudi catalog，那么这里可以不用设置path，hudi会在catalog的catalog.path/current_database目录下创建hudi表文件夹，在这里也就是在/my-hudi/hudi_catalog/dev_db文件夹下创建hudi_hello_world文件夹，作为该表的存储目录
 'path' = 'hdfs://namenode:9000/my-hudi/hudi_catalog/dev_db/hudi_hello_world',
  'payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',
  'hoodie.payload.ordering.field' = 'ts',
  'table.type' = 'MERGE_ON_READ',
  'hoodie.datasource.query.type'='snapshot',
  'read.streaming.enabled'='true',
  'read.start-commit'='earliest',
  'read.streaming.check-interval'='5',
  'read.streaming.skip_compaction'='false',
  'read.streaming.skip_clustering'='false',
  'index.bootstrap.enabled'='true',
  'index.type'='BUCKET',
  'hoodie.index.bucket.engine'='SIMPLE',
  'hoodie.bucket.index.num.buckets'='5',
  'index.state.ttl'='-1',
  'index.global.enabled'='true',
  'write.operation'='upsert',
  'write.precombine'='true',
  'write.tasks'='4',
  'write.task.max.size'='1024',
  'write.batch.size'='256',
-- bulk insert的配置，数据从上游source到下游writer时，是否要进行shuffle，将相同partition的数据发送到同一个writer的subTask中，可以保证相同分区的数据交由同一个writer的subTask写，减少产生的文件数量（如果同一个分区的数据交由多个writer的subTask，每个subTask都会产生一个parquet文件）。
  'write.bulk_insert.shuffle_input'='true',
-- bulk insert的配置，数据在从source shuffle过来后，交由writer写入前，是否要对数据进行排序，以让相同分区或相同分区、hoodie key的数据排在一起。
  'write.bulk_insert.sort_input'='true',
-- bulk insert的配置，如果需要对shuffle过来的数据进行排序，那么该配置决定是按partition排序还是按partition加hoodie key排序。如果为false是按partition排序，否则是按partition加hoodie key排序。
  'write.bulk_insert.sort_input.by_record_key'='true',
-- bulk insert的配置，如果需要对shuffle过来的数据进行排序，那么该配置决定排序动作使用的内存大小。注意：这块内存使用的是flink的托管内存(managed memory)。
  'write.sort.memory'='128',
  'precombine.field'='ts',
    'compaction.trigger.strategy'='num_commits',
    'compaction.delta_commits'='2',
    'compaction.schedule.enabled'='true',
    'compaction.async.enabled'='true',
    'clustering.schedule.enabled'='true',
    'clustering.async.enabled'='false',
    'clustering.delta_commits'='2',
    'clean.async.enabled'='true',
    'clean.policy'='KEEP_LATEST_FILE_VERSIONS',
    'clean.retain_commits'='1',
    'clean.retain_hours'='24',
    'clean.retain_file_versions'='2',
    'hadoop.dfs.replication'='1',
    'hadoop.dfs.client.block.write.replace-datanode-on-failure.policy'='NEVER',
    'hoodie.logfile.data.block.format'='avro',
    'hoodie.metadata.enable'='false',
    'changelog.enabled'='false',
    'hive_sync.enable' = 'true',     -- Required. To enable hive synchronization
  'hive_sync.mode' = 'hms',       -- Required. Setting hive sync mode to hms, default jdbc
  'hive_sync.metastore.uris' = 'thrift://hive-metastore:9083', -- Required. The port need set on hive-site.xml
--   'hive_sync.jdbc_url' = 'jdbc:postgresql://hive-metastore-postgresql/metastore', -- Required. The port need set on hive-site.xml
--   'hive_sync.username' = 'hive', -- Required. The port need set on hive-site.xml
--   'hive_sync.password' = 'hive', -- Required. The port need set on hive-site.xml
  'hive_sync.db'='hello_hudi',                        -- required, hive database name
  'hive_sync.table'='hudi_hello_world'               -- required, hive table name
);

-- 注意：在使用hudi catalog时，不能创建除了hudi以外的connector的表，否则hudi catalog会把所有表的connector修改为hudi
create table default_catalog.default_database.hello_kafka(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    `timeStamp` bigint,
    dt string
)with(
     'connector'='kafka',
     'properties.bootstrap.servers'='kafka-1:9092',
     'topic'='waybill-c',
     'key.format'='raw',
     'key.fields'='waybillCode',
     'value.format'='json',
     'scan.startup.mode'='latest-offset'
 );

-- 注意：在使用hudi catalog时，不能创建除了hudi以外的connector的表，否则hudi catalog会把所有表的connector修改为hudi
create table default_catalog.default_database.hello_filesystem(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    `timeStamp` bigint,
    dt string
)
partitioned by(dt)
with(
     'connector'='filesystem',
     'path'='file:///my-hudi/file_table',
     'format'='json',
    'sink.partition-commit.trigger'='process-time',
    'sink.partition-commit.delay'='0 s',
    'sink.partition-commit.policy.kind'='success-file'
 );

set execution.runtime-mode=streaming;
set execution.runtime-mode=batch;
insert into hudi_hello_world select * from hello_filesystem;
insert into hudi_hello_world select * from default_catalog.default_database.hello_kafka;
insert into default_catalog.default_database.hello_filesystem select * from default_catalog.default_database.hello_kafka;

insert into hudi_hello_world values('500','zhang san',cast(null as int),100,'2023-05-27'),('500',cast(null as string),88,200,'2023-05-27');
insert into hudi_hello_world values('700','niu niu',80,2000,'2023-06-07'),('700',cast(null as string),60,2100,'2023-06-07');

select * from my_hudi_table where dt='2023-05-02'

    insert into hudi_hello_world values('500','zhang san','s1','s1_name',100,'2023-05-27'),('500',cast(null as string),'s2','s2_name',200,'2023-05-27');
set sql-client.execution.result-mode=TABLEAU;

{"waybillCode":"JD0000000092","dt":"2023-06-23","waybillSign":"11001","timeStamp":1687241160801}
{"waybillCode":"JD0000000077","dt":"2023-06-21","siteCode":"111","timeStamp":1687318133562}
{"waybillCode":"JD0000000004","dt":"2023-06-14","siteName":"888站点","timeStamp":1687241160803}
{"waybillCode":"JD000000000100","dt":"2023-06-14","waybillSign":"01000","timeStamp":1687276800008}

update hudi_hello_world /*+ options('read.streaming.enabled'='false') */ set waybillSign='11111000' where waybillCode='JD0000000005';
select * from hudi_hello_world /*+ options('read.streaming.enabled'='false') */ where dt='2023-06-21';
select waybillCode,count(*) from hudi_hello_world /*+ options('read.streaming.enabled'='false') */ group by waybillCode having  count(*)>1;
select waybillCode,count(*) from hudi_hello_world group by waybillCode having  count(*)>1;
select * from hudi_hello_world /*+ options('read.streaming.enabled'='false','read.start-commit'='20230621092035274') */;