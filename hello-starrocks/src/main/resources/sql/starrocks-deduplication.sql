# 需求（背景）
# 有一张数据量很大的数据源表，我们需要统计每个生日日期中有多少个不重复的名称。也就是select birthday,count(distinct name) cnt from mydb.hello_world_source group by birthday
# 需求虽然实现了，但是有一个问题，因为name字段是varchar的，导致在去重统计时性能消耗很大，进而使整个sql执行的非常慢。
# 优化思路：
# 1.设置一个字典表，为name字段的每种值增加一个对应的bigint类型的映射
# 2.source表关联字典表，使source的数据中能够获得name字段的对应bigint类型的映射值
# 3.创建一个聚合表，以统计维度为key字段，以bitmap类型为value字段，使用bitmap_union函数计算
# 4.将source表中统计维度字段(birthday)与需要去重统计的字段的bigint映射值(name_id)写入到聚合表
# 5.查询聚合表，使用bitmap_count获取每个统计维度对应的去重结果

# 准备工作1：创建数据源表
create table mydb.hello_world_source(
    sex int,
    name varchar(10),
    id bigint,
    age int,
    salary int,
    pId int,
    uId int,
    birthday date
)
duplicate key(sex,name)
partition by date_trunc('day',birthday)
distributed by hash(age) buckets 3;

# 准备工作2： 向数据源表导入数据
CREATE ROUTINE LOAD mydb.load_data_hello_world ON hello_world_source
COLUMNS (id,name,age,sex,salary,pId,uId,birthday=years_sub(curdate(),age))
PROPERTIES (
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "30",
    "max_filter_ratio" = "1",
    "format" = "json",
    "jsonpaths" = "[\"$.id\",\"$.name\",\"$.age\",\"$.sex\",\"$.salary\",\"$.pId\",\"$.uId\"]",
    "task_consume_second" = "120",
    "task_consume_second" = "480"
)
FROM kafka
(
    "kafka_broker_list" = "kafka-1:9092",
    "kafka_topic" = "hello_world",
    "property.kafka_default_offsets" = "OFFSET_END",
    "property.group.id" = "sr_routine_group"
);

show routine load for load_data_hello_world;
show routine load task where jobname='load_data_hello_world';
stop routine load for load_data_hello_world;

# 基于auto_increment生成全局字典表，进而进行精准去重
# 1.创建一个字典表（主键模型），存储name字段值及对应的bigint映射值（该字段通过auto_increment生成）
create table mydb.dict_hello_world(
    name varchar(10),
    name_id bigint auto_increment
)
primary key (name)
distributed by hash(name) buckets 1;

# 2.将source中的数据写入到字典表中。
# 注意：由于字典表是主键表，因此在写入前需要将字典中已有key，在source表中刨除掉。避免字典表中原有key的数据被覆盖，导致对应的value被改变。
insert into mydb.dict_hello_world(name)
select distinct name
from mydb.hello_world_source t
where not exists(
    select name from mydb.dict_hello_world where name=t.name
);

# 3.创建一个目标表，与source表类似，区别是多了字典表的映射值字段name_id。该字段是一个计算字段，使用dict_mapping函数获取。
# 也就是说，在向该表导入时，会自动：
# a) 使用要写入的数据中的name字段值，来查询字典表
# b) 将查询字典表获取的映射值赋值到name_id字段
create table mydb.hello_world_dest(
    sex int,
    name varchar(10),
    id bigint,
    age int,
    salary int,
    pId int,
    uId int,
    birthday date,
    name_id bigint as dict_mapping('mydb.dict_hello_world',name,true)
)
duplicate key(sex,name)
partition by date_trunc('day',birthday)
distributed by hash(age) buckets 3;

# 4.将source表数据写入到dest表，以存储name字段对应的映射值字段name_id。后续可以使用name_id字段进行精准去重。
insert into mydb.hello_world_dest
select * from mydb.hello_world_source;

# 5.创建一个聚合表，以统计维度birthday字段作为key，以要统计去重结果的字段的映射值字段name_id作为value。value类型为bitmap，使用的是bitmap_union函数聚合
create table mydb.hello_world_agg_birthday(
    birthday date,
    deduplication_cnt bitmap bitmap_union
)
aggregate key (birthday)
distributed by hash(birthday) buckets 1;

# 6.将dest表中数据写入到聚合表，进行精确去重的统计
insert into mydb.hello_world_agg_birthday
select birthday,name_id
from mydb.hello_world_dest;

# 7.查询聚合表，使用bitmap_count函数就可以获取精准去重的结果
select birthday,bitmap_count(deduplication_cnt) cnt from mydb.hello_world_agg_birthday;

# 简化版（没有上述的步骤3和4）
# 我们可以去掉dest表，直接从source表写入到聚合表，避免明细数据在source表和dest表的重复存储。
# 在查询时使用dict_mapping从字典表中获取映射值。
insert into mydb.hello_world_agg_birthday
select birthday,to_bitmap(dict_mapping('mydb.dict_hello_world',name,true)) name_id
from mydb.hello_world_source;

# 性能对比
# source底表数据量400w：
# 1.直接对source表进行count(distinct name)，需要2s才可以完成。
# 2.按照优化后的方案，查询聚合表，只需要255ms
# 因此可以看到，针对精准去重的场景，当底表数据量过大后，使用优化后的去重方案确实比直接count(distinct)的性能要强好几倍
# 26 rows retrieved starting from 1 in 2 s 429 ms (execution: 2 s 315 ms, fetching: 114 ms)
select birthday,count(distinct name) cnt from mydb.hello_world_source group by birthday;
# 26 rows retrieved starting from 1 in 255 ms (execution: 191 ms, fetching: 64 ms)
select birthday,bitmap_count(deduplication_cnt) cnt from mydb.hello_world_agg_birthday;
