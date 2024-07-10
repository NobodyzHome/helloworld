CREATE
EXTERNAL CATALOG my_paimon_catalog
PROPERTIES
(
    "type" = "paimon",
    'paimon.catalog.type'='filesystem',
    'paimon.catalog.warehouse'='/my-paimon'
);

show catalogs;
set catalog my_paimon_catalog;

show databases;

use app;

show tables;

select *
from paimon_mysql_cdc
where dt = '2024-01-03';
select *
from paimon_table
where dt = '2024-01-02';
select age, count(id) cnt
from paimon_mysql_cdc
where dt = '2024-01-03'
group by age;
select age, count(id) cnt
from student_info
where dt = '2024-01-10'
group by age;
select *
from student_info
where dt = '2024-01-10';

show catalogs;

show databases;

create database mydb;

show databases;

use mydb;

create table mydb.hello_world
(
    ts        datetime not null,
    id        int(10)  not null,
    name      varchar(50),
    age       int(5),
    age_level int(3)
) primary key(ts,id)
partition by range(ts)(
    START ("2024-03-01") END ("2024-03-31") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(id) BUCKETS 2
order by (age)
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true"
);

drop table mydb.hello_world;

show tables;

CREATE
ROUTINE LOAD
mydb.myload ON hello_world
COLUMNS(ts,id,name,age)
PROPERTIES
(
    "desired_concurrent_number" = "5",
    "format" = "json",
    "jsonpaths" = "[\"$.ts\",\"$.id\",\"$.name\",\"$.age\"]"
 )
FROM KAFKA
(
    "kafka_broker_list" = "kafka-1:9092",
    "kafka_topic" = "hello_world",
    "kafka_partitions" = "0,1,2,3,4",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
SHOW ROUTINE LOAD;

STOP
ROUTINE LOAD
FOR myload;

select count(*)
from mydb.hello_world
where ts >= '2024-03-22';

show partitions from mydb.hello_world;

truncate table mydb.hello_world;

EXPORT TABLE mydb.hello_world
TO "hdfs://namenode:9000/export/hello_world/"
PROPERTIES
(
    "column_separator"=",",
    "load_mem_limit"="2147483648",
    "timeout" = "3600"
)
WITH BROKER

SHOW EXPORT;

INSERT into FILES("path" = "hdfs://namenode:9000/export/hello_world/",
                  "format" = "parquet",
                  "compression" = "lz4",
                  "partition_by" = "dt")
SELECT *, cast(cast(ts as date) as string) dt
FROM mydb.hello_world;



INSERT into FILES("path" = "hdfs://namenode:9000/export/hello_world/mydata-",
                  "format" = "parquet",
                  "compression" = "lz4")
SELECT *
FROM mydb.hello_world;

# 使用broker load时，是针对某个库来导入的，所以要加上库名，后面再跟一个自定义的label名称
# 如果当前提交的load任务失败了，则该label名称可以复用。如果该任务成功了，该label名称就不能复用了
LOAD
LABEL mydb.loadFromHdfs4
(
#   可以使用通配符*、?等，将多个文件导入到指定的table中
    DATA INFILE("hdfs://namenode:9000/export/hello_world/mydata-*")
    INTO TABLE hello_world
#  可以将数据导入到正式分区，但前提是有这个正式分区
#  PARTITION(p20240321,p20240322)
#  可以将数据导入到临时分区，但前提是先要创建了这个分区
#  注意：不论是导入正式分区还是临时分区，都要求要导入的数据必须在指定的分区内，如果有数据所在的分区是指定的分区之外，则load任务会报错，导入失败
#  注意：如果没有指定partition，那么就根据导入数据的值来决定要写入到哪个分区
    TEMPORARY PARTITION(tmp20240321,tmp20240322)
    FORMAT AS "parquet"
#   如果需要根据数据源中的某些字段形成一个新的字段，就需要使用列出column list，然后用set来生成一个新的字段
     (ts,id,name,age)
    set (age_level=if(age<=10,1,2))
#   broker load支持where条件
     where age<=20
)
WITH BROKER;

truncate table mydb.hello_world;


LOAD
LABEL mydb.loadFromHdfs5
(
    DATA INFILE("hdfs://namenode:9000/export/hello_world/mydata-18004c37-e80f-11ee-bcf3-0242ac130007_0_1.parquet")
    INTO TABLE hello_world
    FORMAT AS "parquet"
     (ts,id,name,age)
    set (age_level=if(age<=10,1,2))
)
WITH BROKER;

select date(ts) dt,count(*) from mydb.hello_world group by date(ts);


# 可以直接查询hdfs中文件的数据，并进行分析
select date(ts) dt,count(*) cnt
from FILES
     (
        "path" = "hdfs://namenode:9000/export/hello_world/mydata-18004c37-e80f-11ee-bcf3-0242ac130007_0_1.parquet",
        "format" = "parquet"
     )
group by date(ts);

# 创建临时分区
alter table mydb.hello_world
    add TEMPORARY partition tmp20240322 values [("2024-03-22"), ("2024-03-23"));
# 查询所有临时分区
show TEMPORARY partitions from mydb.hello_world;
# 查询临时分区的数据
select *
from mydb.hello_world TEMPORARY partition (tmp20240321);
# 用临时分区替换正式分区，替换时临时分区的范围必须和原分区的范围重合
alter table mydb.hello_world replace partition (p20240322) with TEMPORARY partition (tmp20240322);
# 查询正式分区的数据
select count(*)
from mydb.hello_world partition (p20240321);
update mydb.hello_world
set age=age + 1
where ts >= '2024-03-22';
# 查询所有正式分区
show partitions from mydb.hello_world;
# 查询所有临时分区
show TEMPORARY partitions from mydb.hello_world;


# 从文件路径获取字段值



select to_bitmap(131);

create table test_tbl(
    pg_id int(5),
    usr_id int
)
duplicate KEY(`pg_id`)
DISTRIBUTED BY HASH(`pg_id`);

show tables;

select pg_id,bitmap_count(to_bitmap(usr_id)) from test_tbl group by pg_id;

select * from test_tbl;

insert into test_tbl values(1,1),(1,2),(1,3),(1,1),(2,1);


select pg_id,usr_id,to_bitmap(usr_id) bit_map
from test_tbl;



select pg_id, bitmap_count(bitmap_union(to_bitmap(usr_id))) bitmap_cnt from test_tbl group by pg_id;

create table test_agg_tbl(
    pg_id int,
    usr_id bitmap bitmap_union
)
aggregate key(pg_id)
DISTRIBUTED by hash (pg_id) buckets 1;

insert into test_agg_tbl values(1,to_bitmap(1)),(1,to_bitmap(2)),(1,to_bitmap(3)),(1,to_bitmap(1)),(2,to_bitmap(1));

# 下面这两个是一样的
select pg_id,count(distinct usr_id) from test_agg_tbl group by pg_id;
select pg_id,bitmap_count(usr_id) cnt from test_agg_tbl;
explain select sex,count(*) cnt from mydb.det_test where tm>=curdate() and age=20 group by sex;

select date(tm) dt,count(*) from mydb.det_test group by date(tm);

show routine load from mydb;

use mydb;

select last_query_id();

analyze profile from '7c01b8b8-2309-11ef-a61b-0242ac130003';

desc det_test;

explain analyze select count(*) from pk_test where site_code=1001;

create table det_test2(
    id int,
    name varchar(50),
    age int
)
duplicate key(id);

insert into det_test2 values(1,'zhangsan',10) ,(2,'lisi',11);

explain analyze select * from det_test2 where id=2;

analyze profile from 'b1843a7f-230b-11ef-a61b-0242ac130003';

SHOW PROFILELIST;

use mydb;

show tables;

# 针对明细表，key是用于排序的键
# 分区键可以不是key中的字段
# 分桶键也可以不是key中的字段
# 排序键就是key，无需设置
# 设计明细表，将数据产生时间字段设置为分区字段，将查询必带的字段设置为分桶键，将查询经常带的字段设置为key，也就是排序键
create table det_test3(
    id int,
    name varchar(50),
    age int,
    dt date
)
duplicate key(id)
partition by date_trunc('day',dt)
distributed by hash(name) buckets 2;

# 针对主键表，key就是数据的主键
# 分区键需要是key的范围内的一个字段
# 分桶键也需要是key的范围内的一个字段
# 排序键与key是隔离的，排序键可以key以外的字段
# 设计主键表，就先根据业务情况选择主键；然后从主键中挑出行为或统计时间设置为分区键；然后从主键中挑出查询必带着的字段，设置为分桶键；最后从所有字段中选择查询经常带着的字段，设置为排序键。
create table pk_test2(
    ord_tm datetime,
    waybill_code varchar(100),
    site_code int,
    site_name varchar(100)
)
primary key(ord_tm,waybill_code)
partition by date_trunc('day',ord_tm)
distributed by hash(waybill_code) buckets 1
order by(site_code);

explain analyze select * from pk_test2 where ord_tm=curdate() and waybill_code='JDA' and site_code=1001;

insert into pk_test2 values(now(),'JDA',1001,'1001站点'),(now(),'JDB',1002,'1002站点'),(now(),'JDC',1003,'1003站点');

select count(*) from mydb.det_test;

create table mydb.stu_info(
    id int,
    age int,
    name varchar(50),
    sex int,
    tm datetime,
    index sex_i (sex) using bitmap
)
DUPLICATE KEY(id)
partition by range(tm)(
    start ('2024-06-13') end ('2024-06-16') every (interval 1 day)
)
DISTRIBUTED by hash(age) buckets 2;

show databases;

insert into mydb.stu_info values(1,10,'hello',1,now());
show partitions from mydb.stu_info;

drop table  mydb.stu_info;

explain analyze select sex,count(*) cnt from mydb.stu_info where tm>=curdate()  and id<0 and sex=1 group by sex;

show index from mydb.stu_info;

create routine load mydb.stu_info_routine_10 on stu_info
columns(id,age,name,sex,tm)
PROPERTIES
(
    "desired_concurrent_number" = "2",
    "format" = "json",
    "jsonpaths" = "[\"$.id\",\"$.age\",\"$.name\",\"$.sex\",\"$.tm\"]"
 )
FROM KAFKA
(
    "kafka_broker_list" = "kafka-1:9092",
    "kafka_topic" = "hello_starrocks",
    "kafka_partitions" = "0,1,2,3,4",
    "property.kafka_default_offsets" = "OFFSET_END"
);

show routine load from mydb;

use mydb;

stop routine load for stu_info_routine_10;

select * from mydb.stu_info;

create table mydb.stu_info_list_partition(
    id int,
    classroom varchar(50),
    age int,
    name varchar(50),
    sex int
)
primary key(id,classroom,age)
partition by list(classroom)(
    partition p1 values in('1-1','1-2','1-3'),
    partition p2 values in('2-1','2-2','2-3'),
    partition p3 values in('3-1','3-2'),
    partition p4 values in('4-1','4-2','4-3','4-4'),
    partition p5 values in('5-1','5-2')
)
distributed by hash(age) buckets 1
order by(sex);

drop table mydb.stu_info_list_partition;

create table mydb.stu_info_list_partition(
    id int,
    name varchar(50),
    sex int,
    classroom varchar(50),
    age int
)
primary key(id,name,sex)
partition by list(name)(
    partition pL values in('li','liu'),
    partition pZ values in('zhang','zhao'),
    partition pW values in('wang')
)
distributed by hash(sex) buckets 1
order by(age);

show partitions from mydb.stu_info_list_partition;

insert into mydb.stu_info_list_partition values (5,'liu',2,'3-1',10),(6,'liu',1,'2-2',11);

explain select classroom,count(*) cnt from mydb.stu_info_list_partition where name in('liu','zhao') group by classroom;

drop table mydb.app_kd_pending_sum_1h;

create table mydb.app_kd_pending_sum_1h(
    dt date,
    province_agency_code int,
    link_type int,
    delay_cnt  int sum,
    timely_rate double max
)
aggregate key(dt,province_agency_code,link_type)
partition by range(dt)(
    start ('2024-06-13') end ('2024-06-19') every (interval 1 day)
)
distributed by hash(link_type) buckets 2;

insert into mydb.app_kd_pending_sum_1h values ('2024-06-14',100,2,17,15.5),('2024-06-14',100,2,11,3.5),('2024-06-14',100,2,15,20.3),('2024-06-14',50,1,33,19.1),('2024-06-14',300,1,22,25.5);
select * from mydb.app_kd_pending_sum_1h;

explain analyze select province_agency_code,sum(delay_cnt) from mydb.app_kd_pending_sum_1h where dt='2024-06-14' and link_type>0 group by province_agency_code ;

show partitions from mydb.app_kd_pending_sum_1h;


insert into mydb.stu_info_list_partition values(1,'zhang',1,'1-3',10);
show partitions from mydb.stu_info_list_partition;

select * from mydb.stu_info_list_partition;

# list分区的应用，例如将classroom为'1-1','1-2','1-3'的数据都放到pG1这个分区中，通过classroom字段将不同年级的数据分到不同的分区中
create table mydb.stu_report(
    classroom varchar(50) not null,
    stu_cnt int sum,
    max_age int max,
    min_age int min
)
aggregate key(classroom)
partition by list(classroom)(
    partition pG1 values in('1-1','1-2','1-3'),
    partition pG2 values in('2-1','2-2','2-3'),
    partition pG3 values in('3-1','3-2'),
    partition pG4 values in('4-1','4-2','4-3','4-4'),
    partition pG5 values in('5-1','5-2')
)
distributed by random buckets 1;

insert into mydb.stu_report values('1-1',1,10,10),('1-1',1,11,11),('1-1',1,12,12),('1-1',1,12,12);
insert into mydb.stu_report values('2-1',1,13,13),('2-1',1,16,16),('2-2',1,19,19),('2-2',1,6,6);

# 查询二年级这个分区的所有的数据
select * from mydb.stu_report partition (pG2);

create table mydb.express_datetime_partition(
    age int,
    tm datetime,
    name varchar(50),
    classroom varchar(20)
)
duplicate key(age)
partition by date_trunc('day',tm)
distributed by hash(name)
properties(
    'partition_live_number'='3'
);

drop table mydb.express_datetime_partition;

show partitions from mydb.express_datetime_partition;

insert into mydb.express_datetime_partition values(10,now(),'zhangsan','1-1'),(11,now()-interval 1 day,'lisi','1-2'),(15,now()-interval 2 day,'wangwu','2-1'),(16,now()-interval 3 day,'zhaoliu','2-2'),(18,now()-interval 4 day,'balala','3-1');
show create table mydb.express_datetime_partition;

create table mydb.express_datetime_partition_week(
    id int,
    create_tm datetime,
    name varchar(50),
    age int,
    sex int
)
primary key(id,create_tm,name)
partition by time_slice(create_tm,interval 7 day)
distributed by hash(name)
order by (age)
properties(
    'partition_live_number'='3'
);

insert into mydb.express_datetime_partition_week values(1,now(),'zhangsan',10,1),(1,now()- interval 9 day,'lisi',20,2);
show partitions from mydb.express_datetime_partition_week;

create table mydb.express_column_partition(
    id int,
    name varchar(50),
    age int,
    sex int,
    classroom varchar(50) not null
)
duplicate key(id)
partition by (classroom)
distributed by random buckets 2;

show partitions from mydb.express_column_partition;

insert into mydb.express_column_partition values(4,'wangwu',18,1,'2-1'),(5,'zhaoliu',20,2,'2-2');

select * from mydb.express_column_partition partition(p12);

create table mydb.express_column_partition_expire(
    id int,
    name varchar(50),
    province varchar(20) not null,
    city varchar(20) not null
)
duplicate key(id)
partition by (province,city)
properties(
    'partition_live_number'='3'
);

drop table mydb.express_column_partition_expire;

insert into mydb.express_column_partition_expire values(10,'xx','he bei','tang shan');


show partitions from mydb.express_column_partition_expire;


create table mydb.site_visit_record(
   visit_dt date not null,
    page_id int,
    user_id bitmap bitmap_union
)
aggregate key(visit_dt,page_id)
partition by (visit_dt)
distributed by hash(page_id) buckets 2;

drop table mydb.site_visit_record;

insert into mydb.site_visit_record values(curdate(),1,to_bitmap(100)),(curdate(),1,to_bitmap(101)),(curdate(),1,to_bitmap(15));
insert into mydb.site_visit_record values(curdate(),1,to_bitmap(100)),(curdate(),1,to_bitmap(3)),(curdate(),1,to_bitmap(101));
insert into mydb.site_visit_record values(curdate(),1,to_bitmap(15)),(curdate(),1,to_bitmap(5)),(curdate(),1,to_bitmap(20));
insert into mydb.site_visit_record values(curdate(),2,to_bitmap(39)),(curdate(),1,to_bitmap(17)),(curdate(),2,to_bitmap(11));
insert into mydb.site_visit_record values(curdate(),2,to_bitmap(20)),(curdate(),2,to_bitmap(11)),(curdate(),2,to_bitmap(11));



select bitmap_to_string(user_id) from mydb.site_visit_record;
select visit_dt,page_id,bitmap_count(user_id) cnt from mydb.site_visit_record;
select visit_dt,page_id,BITMAP_INTERSECT(user_id) cnt from mydb.site_visit_record;

select visit_dt,bitmap_to_string(BITMAP_INTERSECT(user_id)) cnt from site_visit_record group by visit_dt;
select visit_dt,bitmap_to_string(bitmap_union(user_id)) cnt from site_visit_record group by visit_dt;

show partitions from mydb.site_visit_record;

show BUILTIN functions like '%bitmap%';

# show frontends;

# routine load中，columns中字段的顺序需要和proeprties中jsonpaths的顺序一致，但是不用和建表语句中字段的顺序一致
# __op字段是sr用来判断如何对数据进行操作的字段（update还是delete），建表时不用带着这个字段，导入时可以带着这个字段，用于告诉sr这条数据是要删除还是更新
# 下例中，我们通过if函数动态给__op字段赋值，当__op=0时，代表这条数据需要更新，当__op=1时，代表这条数据需要被删除
# 即使一条要导入的数据的__op=1，这条数据也需要满足表的约束才能成功被导入，例如表中约束sex字段不能，那么导入数据时，即使__op=1，但是数据中没有sex字段的话也会报错
# 当要导入的数据在表中不存在，且这条数据的__op=1，那么这条数据实际不会写到表中
create routine load mydb.stu_info_routine_17 on stu_info_pk
columns(id,age,name,sex,tm,__op=if(age>20,1,0))
PROPERTIES
(
    "desired_concurrent_number" = "2",
    "format" = "json",
    "jsonpaths" = "[\"$.id\",\"$.age\",\"$.name\",\"$.sex\",\"$.tm\"]"
 )
FROM KAFKA
(
    "kafka_broker_list" = "kafka-1:9092",
    "kafka_topic" = "hello_starrocks",
    "kafka_partitions" = "0,1,2,3,4",
    "property.kafka_default_offsets" = "OFFSET_END"
);
# 查询所有的routine load
show routine load;
# 关闭指定的routine load，需要指定db以及对应的routine load的label
stop routine load for mydb.stu_info_routine_12;

select * from stu_info_pk;

show create table mydb.stu_info;

CREATE TABLE `stu_info_pk` (
    `id` int(11) not NULL COMMENT "",
    `sex` int(11) not NULL COMMENT "",
    `age` int(11) not NULL COMMENT "",
    `name` varchar(50) NULL COMMENT "",
    `tm` datetime NULL COMMENT ""
)
primary key(id,sex)
distributed by hash(sex) buckets 2
order by (age);

{"id":"1","age":15,"name":"lisi","sex":1,"tm":"2024-06-26 09:38:48.0"}
{"id":"2","age":16,"name":"wangwu","sex":2,"tm":"2024-06-26 09:38:48.0"}
{"id":"3","age":17,"name":"zhaoliu","sex":1,"tm":"2024-06-26 09:38:48.0"}
{"id":"3","age":21,"name":"zhangsan","sex":1,"tm":"2024-06-26 08:38:48.0"}
{"id":"9","age":23,"name":"test","sex":1,"tm":"2024-06-26 08:38:48.0"}


select tracking_log from information_schema.load_tracking_logs where job_id=16061;

show databases;
create database mydb;
show tables from mydb;