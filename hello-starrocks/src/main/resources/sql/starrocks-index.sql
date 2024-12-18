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

SET enable_profile = true;

select * from mydb.realtime_delivery_invocation_test_index where dt='2024-10-28' and apiName='deliveryMonitoMidOperBp';

SHOW ALTER TABLE COLUMN FROM mydb;

SHOW INDEX FROM mydb.realtime_delivery_invocation_test_index;

select * from mydb.realtime_delivery_invocation_test_index where apiName='deliveryMonitoMidOperBp'