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

create table hello_world(id string,name string,proc_time as PROCTIME()) with('connector'='kafka','properties.bootstrap.servers'='kafka-1:9092','topic'='hello_world','scan.startup.mode'='earliest-offset','key.format'='raw','key.fields'='id','value.format'='json');

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



