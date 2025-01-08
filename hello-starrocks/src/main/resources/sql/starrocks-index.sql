create table mydb.hello_world(
    id bigint,
    name varchar(10),
    age int,
    sex int,
    salary int,
    pId int,
    uId int,
    INDEX pId_index (pId) USING BITMAP,
    INDEX uId_index (uId) USING BITMAP
)
duplicate key (id,name)
distributed by random buckets 1
properties(
    "bloom_filter_columns" = "salary"
);

SHOW ALTER TABLE COLUMN FROM mydb;
SHOW INDEX FROM mydb.hello_world;



drop table mydb.hello_world;
truncate table mydb.hello_world;

use mydb;

CREATE ROUTINE LOAD mydb.load_hello_world_1 ON hello_world
PROPERTIES(
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "30",
    "max_error_number" = "5",
    "format" = "json",
    "task_consume_second" = "5",
    "task_timeout_second" = "20"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka-1:9092",
    "kafka_topic" = "hello_world",
    "property.kafka_default_offsets" = "OFFSET_END",
    "property.group.id" = "sr_routine_load_hello_world"
);

use mydb;
show routine load for load_hello_world_1;
show routine load task where jobName='load_hello_world_1';
stop routine load for load_hello_world_1;

select count(*) from hello_world group by id having count(*)>1;
select max(id) from hello_world;
select * from hello_world where salary=6350 and id =4497452;
select * from hello_world where id=202 and name='8c780';
select * from hello_world where id=855512 and name='551ce';
select * from hello_world where salary=12614;
select * from hello_world where id=202 and salary=3469;
select * from hello_world where id=5382177 or id=1617;

SET enable_profile = true;

show tablet from hello_world;

ANALYZE PROFILE FROM '5f856475-ac85-11ef-86a9-0242c0a83006';



create table mydb.realtime_delivery_invocation_test_index(
    dt date,
    apiName varchar(500),
    invoke_tm datetime,
    apiGroupName varchar(500),
    appId varchar(500),
    erp varchar(500),
    endDate varchar(500),
    theaterCode varchar(500),
    waybillSource varchar(500),
    deliveryType varchar(500),
    siteName varchar(500),
    deliveryThirdType varchar(500),
    udataLimit varchar(500),
    province_code varchar(500),
    isExpress varchar(500),
    productSubType varchar(500),
    goodsType varchar(500),
    isKa varchar(500),
    areaCode varchar(500),
    orgCode varchar(500),
    partitionCode varchar(500),
    deliverySubType varchar(500),
    rejectionRoleId varchar(500),
    isZy varchar(500),
    productType varchar(500),
    siteDimension varchar(500),
    waybillDimension varchar(500),
    rdm int,
    INDEX rdm_index (rdm) USING BITMAP
)
duplicate key(dt,apiName)
distributed by random buckets 1
properties(
    "bloom_filter_columns" = "erp",
    "replication_num" = "3",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
# 建表后为指定字段增加bitmap索引
CREATE INDEX siteDimension_index on mydb.realtime_delivery_invocation_test_index (siteDimension) USING BITMAP;
# 索引建立是异步执行的，通过show alter table查询索引执行进度
# +-----+---------------------------------------+-------------------+-------------------+---------------------------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
# |JobId|TableName                              |CreateTime         |FinishTime         |IndexName                              |IndexId|OriginIndexId|SchemaVersion|TransactionId|State   |Msg|Progress|Timeout|
# +-----+---------------------------------------+-------------------+-------------------+---------------------------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
# |31564|realtime_delivery_invocation_test_index|2024-12-03 14:33:33|2024-12-03 14:33:45|realtime_delivery_invocation_test_index|31565  |31344        |1:719839061  |40333        |FINISHED|   |null    |86400  |
# +-----+---------------------------------------+-------------------+-------------------+---------------------------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
SHOW ALTER TABLE COLUMN from mydb;

LOAD LABEL mydb.load_log_new_2
(
    DATA INFILE("file:///my-starrocks/realtime_invocation_log/*.log")
    INTO TABLE realtime_delivery_invocation_test_index
     format as "json"
    (apiName,invoke_tm,apiGroupName,appId,erp,endDate,theaterCode,waybillSource,deliveryType,siteName,deliveryThirdType,udataLimit,province_code,isExpress,productSubType,goodsType,isKa,areaCode,orgCode,partitionCode,deliverySubType,rejectionRoleId,isZy,productType,siteDimension,waybillDimension)
     set(dt=cast(invoke_tm as date),rdm=floor(rand()*100000))
)
WITH BROKER
PROPERTIES
(
    "timeout" = "3600",
     "max_filter_ratio"="0",
     "jsonpaths" = "[\"$.apiName\",\"$.invoke_tm\",\"$.apiGroupName\",\"$.appId\",\"$.erp\",\"$.params.endDate\",\"$.params.theaterCode\",\"$.params.waybillSource\",\"$.params.deliveryType\",\"$.params.siteName\",\"$.params.deliveryThirdType\",\"$.params.udataLimit\",\"$.params.province_code\",\"$.params.isExpress\",\"$.params.productSubType\",\"$.params.goodsType\",\"$.params.isKa\",\"$.params.areaCode\",\"$.params.orgCode\",\"$.params.partitionCode\",\"$.params.deliverySubType\",\"$.params.rejectionRoleId\",\"$.params.isZy\",\"$.params.productType\",\"$.params.siteDimension\",\"$.params.waybillDimension\"]"
);

show load where label='load_log_new_2';

drop table mydb.realtime_delivery_invocation_test_index;

select apiName from mydb.realtime_delivery_invocation_test_index where erp='liangdezhi3';
select apiName from mydb.realtime_delivery_invocation_test_index where siteDimension=0;
select apiName from mydb.realtime_delivery_invocation_test_index where erp='liangdezhi3' and siteDimension=1 and rejectionRoleId <> '';
select * from mydb.realtime_delivery_invocation_test_index where isZy=0;
select apiName,dt,rdm,siteDimension from mydb.realtime_delivery_invocation_test_index where rdm in (2001,2168,494,71905) and dt='2024-11-01';
select apiName,dt,erp from mydb.realtime_delivery_invocation_test_index where siteDimension = 5 and rdm in (32176,76750,95856,81753,98348,70722,25568,5859,1455,29889,80023,82404,32594,83374,16677,73589,91784,19441,99135,64310,51049,93978,16825,84498,45987,34160,23366,8061,15340,30717,54130,53876,76249,54947,64732,51677,84842,92931) and erp ='daiqingfu';

select * from mydb.realtime_delivery_invocation_test_index where dt='2024-10-28' and apiName='deliveryMonitoMidOperBp';

SHOW ALTER TABLE COLUMN FROM mydb;

SHOW INDEX FROM mydb.realtime_delivery_invocation_test_index;

select * from mydb.realtime_delivery_invocation_test_index where apiName='deliveryMonitoMidOperBp';


# 如果要去看sql走没走索引，需要启动该配置。然后在查询执行后，通过FE页面的query标签页来查看profile。
SET enable_profile = true;

# 全表数据500w
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

# 给字段设置索引的主要条件
# 1.该字段是否被经常查询
# 2.该字段是否具有较高的基数

# 建表后为表创建bitmap索引
CREATE INDEX lo_salary_index ON hello_world_source (salary) USING BITMAP;
CREATE INDEX lo_pId_index ON hello_world_source (pId) USING BITMAP;
# 索引创建是异步过程，通过SHOW ALTER TABLE COLUMN查询创建过程
# 索引创建中，通过progress字段查询创建进度
# +-----+------------------+-------------------+----------+------------------+-------+-------------+-------------+-------------+-------+---+--------+-------+
# |JobId|TableName         |CreateTime         |FinishTime|IndexName         |IndexId|OriginIndexId|SchemaVersion|TransactionId|State  |Msg|Progress|Timeout|
# +-----+------------------+-------------------+----------+------------------+-------+-------------+-------------+-------------+-------+---+--------+-------+
# |40096|hello_world_source|2024-12-26 10:22:09|null      |hello_world_source|40097  |33495        |1:1181472362 |47106        |RUNNING|   |26/477  |86400  |
# +-----+------------------+-------------------+----------+------------------+-------+-------------+-------------+-------------+-------+---+--------+-------+
# 索引创建完毕
# +-----+------------------+-------------------+-------------------+------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
# |JobId|TableName         |CreateTime         |FinishTime         |IndexName         |IndexId|OriginIndexId|SchemaVersion|TransactionId|State   |Msg|Progress|Timeout|
# +-----+------------------+-------------------+-------------------+------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
# |40096|hello_world_source|2024-12-26 10:22:09|2024-12-26 10:30:39|hello_world_source|40097  |33495        |1:1181472362 |47106        |FINISHED|   |null    |86400  |
# +-----+------------------+-------------------+-------------------+------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
SHOW ALTER TABLE COLUMN from mydb;
# 查询表的索引
# +-----------------------+----------+---------------+------------+-----------+---------+-----------+--------+------+----+----------+-------+
# |Table                  |Non_unique|Key_name       |Seq_in_index|Column_name|Collation|Cardinality|Sub_part|Packed|Null|Index_type|Comment|
# +-----------------------+----------+---------------+------------+-----------+---------+-----------+--------+------+----+----------+-------+
# |mydb.hello_world_source|          |lo_salary_index|            |salary     |         |           |        |      |    |BITMAP    |       |
# |mydb.hello_world_source|          |lo_pId_index   |            |pId        |         |           |        |      |    |BITMAP    |       |
# +-----------------------+----------+---------------+------------+-----------+---------+-----------+--------+------+----+----------+-------+
SHOW index FROM mydb.hello_world_source;
# 删除索引
DROP INDEX lo_age_index ON mydb.hello_world_source;
# 通过profile可以看到，该查询使用bitmap索引，在读取数据文件前就过滤了500w的数据
# 1 row retrieved starting from 1 in 373 ms (execution: 344 ms, fetching: 29 ms)
# - BitmapIndexFilterRows: 5.005M (5004899)
# - __MAX_OF_BitmapIndexFilterRows: 385.657K (385657)
# - __MIN_OF_BitmapIndexFilterRows: 0
select * from mydb.hello_world_source where salary in (2473,122947) and pid=8165;

# 建表后为表创建bloom filter索引
ALTER TABLE mydb.hello_world_source SET ("bloom_filter_columns" = "uId,age");
# 查询索引的创建进度
# +-----+------------------+-------------------+-------------------+------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
# |JobId|TableName         |CreateTime         |FinishTime         |IndexName         |IndexId|OriginIndexId|SchemaVersion|TransactionId|State   |Msg|Progress|Timeout|
# +-----+------------------+-------------------+-------------------+------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
# |41662|hello_world_source|2024-12-26 10:36:01|2024-12-26 10:36:42|hello_world_source|41663  |41023        |3:377501283  |48028        |FINISHED|   |null    |86400  |
# |42300|hello_world_source|2024-12-26 10:37:59|2024-12-26 10:38:43|hello_world_source|42301  |41663        |4:193572567  |48031        |FINISHED|   |null    |86400  |
# |42946|hello_world_source|2024-12-26 10:57:49|null               |hello_world_source|42947  |42301        |5:1712167065 |48045        |RUNNING |   |85/477  |86400  |
# +-----+------------------+-------------------+-------------------+------------------+-------+-------------+-------------+-------------+--------+---+--------+-------+
SHOW ALTER TABLE COLUMN from mydb;
# 使用bloom filter的字段过滤了100w的数据
# 500 rows retrieved starting from 1 in 1 s 373 ms (execution: 1 s 275 ms, fetching: 98 ms)
# - BloomFilterFilterRows: 1.090M (1089771)
# - __MAX_OF_BloomFilterFilterRows: 194.656K (194656)
# - __MIN_OF_BloomFilterFilterRows: 0
select * from mydb.hello_world_source where uid = 8622;
# 联合使用bitmap索引和bloom filter索引，可以发现bitmap索引过滤了大量的数据，bloom filter索引也过滤了一小部分数据
# 5 rows retrieved starting from 1 in 388 ms (execution: 353 ms, fetching: 35 ms)
# - BitmapIndexFilterRows: 5.005M (5004850)
# - __MAX_OF_BitmapIndexFilterRows: 577.533K (577533)
# - __MIN_OF_BitmapIndexFilterRows: 0
# - BloomFilterFilterRows: 12
# - __MAX_OF_BloomFilterFilterRows: 2
# - __MIN_OF_BloomFilterFilterRows: 0
select * from mydb.hello_world_source where uid = 8622 and salary in (361121,13955,281199,280131,473961);
# 联合使用前缀索引、bitmap索引、bloom filter索引，可以看到前缀索引过滤了250w数据，bitmap索引过滤了250w数据，bloom filter索引也过滤了一部分数据
# 4 rows retrieved starting from 1 in 678 ms (execution: 625 ms, fetching: 53 ms)
# - ShortKeyFilterRows: 2.503M (2502772)
# - __MAX_OF_ShortKeyFilterRows: 289.054K (289054)
# - __MIN_OF_ShortKeyFilterRows: 0
# - BitmapIndexFilterRows: 2.502M (2502097)
# - __MAX_OF_BitmapIndexFilterRows: 287.600K (287600)
# - __MIN_OF_BitmapIndexFilterRows: 0
# - BloomFilterFilterRows: 8
# - __MAX_OF_BloomFilterFilterRows: 2
# - __MIN_OF_BloomFilterFilterRows: 0
select * from mydb.hello_world_source where sex=1 and uid = 8622 and salary in (361121,13955,281199,280131,473961);
# 使用完整前缀查询。可以看到通过前缀索引过滤了380w数据，通过zonemap索引过滤了110w数据
# 207 rows retrieved starting from 1 in 194 ms (execution: 125 ms, fetching: 69 ms)
# - SegmentZoneMapFilterRows: 1.116M (1116190)
# - __MAX_OF_SegmentZoneMapFilterRows: 138.102K (138102)
# - __MIN_OF_SegmentZoneMapFilterRows: 0
# - ShortKeyFilterRows: 3.889M (3888503)
# - __MAX_OF_ShortKeyFilterRows: 440.125K (440125)
# - __MIN_OF_ShortKeyFilterRows: 0
select * from mydb.hello_world_source where sex=1 and name='000';
# 使用zonemap索引查询。可以看到使用segment级别的zonemap索引过滤了442w的数据，通过DataPage级别的zonemap索引过滤了55w数据
# 3 rows retrieved starting from 1 in 1 s 289 ms (execution: 879 ms, fetching: 410 ms)
# - SegmentRuntimeZoneMapFilterRows: 0
#                  - SegmentZoneMapFilterRows: 4.428M (4427841)
#                    - __MAX_OF_SegmentZoneMapFilterRows: 769.351K (769351)
#                    - __MIN_OF_SegmentZoneMapFilterRows: 0
#                  - ZoneMapIndexFilterRows: 552.483K (552483)
#                    - __MAX_OF_ZoneMapIndexFilterRows: 184.852K (184852)
#                    - __MIN_OF_ZoneMapIndexFilterRows: 0
select * from mydb.hello_world_source where id between 100 and 102;

show create table mydb.hello_world_source;

show indexes  from mydb.hello_world_source;


CREATE TABLE `hello_world_source` (
                                      `sex` int(11) NULL COMMENT "",
                                      `name` varchar(10) NULL COMMENT "",
                                      `id` bigint(20) NULL COMMENT "",
                                      `age` int(11) NULL COMMENT "",
                                      `salary` int(11) NULL COMMENT "",
                                      `pId` int(11) NULL COMMENT "",
                                      `uId` int(11) NULL COMMENT "",
                                      `birthday` date NULL COMMENT "",
                                      INDEX lo_salary_index (`salary`) USING BITMAP COMMENT '',
                                      INDEX lo_pId_index (`pId`) USING BITMAP COMMENT ''
) ENGINE=OLAP
    DUPLICATE KEY(`sex`, `name`)
PARTITION BY date_trunc('day', birthday)
DISTRIBUTED BY HASH(`age`) BUCKETS 3
PROPERTIES (
"bloom_filter_columns" = "age, uId",
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "3"
);