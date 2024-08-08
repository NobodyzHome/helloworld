use mydb;

create table mydb.base_table1(
    dt date,
    id int,
    name varchar(50),
    age int,
    sex int
)
primary key(dt,id)
partition by date_trunc('day',dt)
distributed by hash(id);

create table dim_table_sex(
    id int,
    name varchar(10)
)
duplicate key(id)
distributed by hash(id) buckets 1;

drop materialized view mydb.dwd_view;

create materialized view mydb.dwd_view
partition by dt
distributed by hash(id) buckets 2
refresh async
properties(
	"replication_num"="1"
)
as
select
    t1.dt,
    t1.id,
    t1.name,
    t1.age,
    t1.sex,
    t2.name sex_name,
    now() ts
from mydb.base_table1 t1,mydb.dim_table_sex t2
where t1.sex=t2.id;


show tables from mydb;

select * from information_schema.tasks;
select * from information_schema.task_runs where task_name='mv-14163';

select * from mydb.dwd_view;

insert into mydb.dim_table_sex values(4,'male2');

insert into mydb.base_table1 values(curdate(),1,'zhang zhang',20,1);
insert into mydb.base_table1 values(curdate()-interval 1 day,2,'li si',30,2);
insert into mydb.base_table1 values(curdate()-interval 3 day,3,'wang wu',35,3);

show partitions from mydb.dim_table_sex;

select * from mydb.dim_table_sex;



# 本章内容：创建一个定时自动刷新的分区物化视图
# 1.创建定时刷新的异步物化视图，通过refresh async every (interval 1 minute)指定任务执行间隔
create materialized view mydb.mv_refresh_period
partition by dt
distributed by hash(id) buckets 1
refresh async every (interval 1 minute)
as
select
    t1.dt,
    t1.id,
    t1.name,
    t1.age,
    t1.sex,
    t2.name sex_name,
    now() ts
from mydb.base_table1 t1,mydb.dim_table_sex t2
where t1.sex=t2.id;
# 2.通过materialized_views表获取异步物化视图的task_name
# +-----------------+---------+
# |table_name       |task_name|
# +-----------------+---------+
# |mv_refresh_period|mv-14970 |
# +-----------------+---------+
select table_name,task_name from information_schema.materialized_views where table_name='mv_refresh_period';
# 3.通过tasks表确认视图对应的任务调度类型是定时调度
# +---------+-------------------------------------------------------+
# |task_name|schedule                                               |
# +---------+-------------------------------------------------------+
# |mv-14970 |PERIODICAL (START 2024-08-07T13:19:08 EVERY(1 MINUTES))|
# +---------+-------------------------------------------------------+
select task_name,schedule from information_schema.tasks where task_name='mv-14970';
# 4.通过task_runs表查看每次触发任务的执行情况，确认是1分钟1次
# +------------------------------------+---------+-------------------+-------------------+-------+--------+--------+
# |query_id                            |task_name|create_time        |finish_time        |state  |database|progress|
# +------------------------------------+---------+-------------------+-------------------+-------+--------+--------+
# |9a6fb203-54c0-11ef-8296-0242ac120003|mv-14970 |2024-08-07 13:26:07|2024-08-07 13:26:07|SUCCESS|mydb    |100%    |
# |76b2fb82-54c0-11ef-8296-0242ac120003|mv-14970 |2024-08-07 13:25:07|2024-08-07 13:25:07|SUCCESS|mydb    |100%    |
# |52f6460e-54c0-11ef-8296-0242ac120003|mv-14970 |2024-08-07 13:24:07|2024-08-07 13:24:07|SUCCESS|mydb    |100%    |
# |2f39b6ab-54c0-11ef-8296-0242ac120003|mv-14970 |2024-08-07 13:23:07|2024-08-07 13:23:07|SUCCESS|mydb    |100%    |
# |0b7d001b-54c0-11ef-8296-0242ac120003|mv-14970 |2024-08-07 13:22:07|2024-08-07 13:22:07|SUCCESS|mydb    |100%    |
# +------------------------------------+---------+-------------------+-------------------+-------+--------+--------+
select query_id,task_name,create_time,finish_time,state,`database`,progress from information_schema.task_runs where task_name='mv-14970';
# 5.当触发定时刷新后，并不是直接将所有分区刷新，而是关联分区的基表的哪些分区产生变化了，才会刷新哪个分区。因此在不改变基表数据的情况下，即使触发了自动刷新，实际也不会进行刷新，从ts字段不会被改变就可以证明。
# +----------+--+---------+---+---+--------+---------------------+
# |dt        |id|name     |age|sex|sex_name|ts                   |
# +----------+--+---------+---+---+--------+---------------------+
# |2024-07-30|1 |zhang san|20 |1  |男       |2024-07-31 12:31:58.0|
# |2024-07-28|3 |wang wu  |35 |3  |male1   |2024-07-31 12:31:58.0|
# |2024-07-29|2 |li si    |30 |2  |male    |2024-07-31 12:31:58.0|
# +----------+--+---------+---+---+--------+---------------------+
select * from mydb.mv_refresh_period;
# 6.向关联分区的基表的2024-07-30分区插入一条数据，会导致物化视图中2024-07-30分区被刷新
insert into mydb.base_table1 values('2024-07-30',10,'zhao liu',22,4);
# 7.再次查询视图，发现2024-07-30分区的数据中的ts被修改了，而其他分区中的数据没有变，说明定时刷新物化视图时，只有基表中有改变的分区才会被刷新，没有改变的分区不会被刷新。
# +----------+--+---------+---+---+--------+---------------------+
# |dt        |id|name     |age|sex|sex_name|ts                   |
# +----------+--+---------+---+---+--------+---------------------+
# |2024-07-28|3 |wang wu  |35 |3  |male1   |2024-07-31 12:31:58.0|
# |2024-07-30|1 |zhang san|20 |1  |男       |2024-07-31 12:40:20.0|
# |2024-07-30|10|zhao liu |22 |4  |male2   |2024-07-31 12:40:20.0|
# |2024-07-29|2 |li si    |30 |2  |male    |2024-07-31 12:31:58.0|
# +----------+--+---------+---+---+--------+---------------------+
select * from mydb.mv_refresh_period;
# 8.当视图变为失效状态后，依然会定时触发任务，但任务直接执行失败
alter materialized view mydb.mv_refresh_period inactive;
# +------------------------------------+---------+-------------------+-------------------+-------+--------+--------+----------+---------------------------------------------------------------------------------------------------------------------------------------------------+
# |query_id                            |task_name|create_time        |finish_time        |state  |database|progress|error_code|error_message                                                                                                                                      |
# +------------------------------------+---------+-------------------+-------------------+-------+--------+--------+----------+---------------------------------------------------------------------------------------------------------------------------------------------------+
# |70db5c2a-54c1-11ef-8296-0242ac120003|mv-14970 |2024-08-07 13:32:07|2024-08-07 13:32:07|FAILED |mydb    |0%      |-1        |com.starrocks.sql.common.DmlException: Materialized view: mv_refresh_period, id: 14970 is not active, skip sync partition and data with base tables|
# |4d1ea5bd-54c1-11ef-8296-0242ac120003|mv-14970 |2024-08-07 13:31:07|2024-08-07 13:31:07|FAILED |mydb    |0%      |-1        |com.starrocks.sql.common.DmlException: Materialized view: mv_refresh_period, id: 14970 is not active, skip sync partition and data with base tables|
# |2962165b-54c1-11ef-8296-0242ac120003|mv-14970 |2024-08-07 13:30:07|2024-08-07 13:30:07|SUCCESS|mydb    |100%    |0         |null                                                                                                                                               |
# +------------------------------------+---------+-------------------+-------------------+-------+--------+--------+----------+---------------------------------------------------------------------------------------------------------------------------------------------------+
select query_id,task_name,create_time,finish_time,state,`database`,progress,error_code,error_message from information_schema.task_runs where task_name='mv-14970';

# 删除一个物化视图
drop materialized view mydb.dwd_view;

# 本章内容：创建和基表分区一比一映射的分区物化视图
# 当创建一个分区物化视图时，需要指定一个基表，使用该基表的分区和物化视图的分区做关联。如果我们直接用基表的分区字段作为视图的分区字段，那么基表每有一个分区，物化视图对应就有同样的一个分区。注意：一个分区物化视图只能和一个基表的分区做关联，不能和多个基表的分区做关联。
# partition by dt，说明：
# 1.用mydb.base_table1表和物化视图关联分区
# 2.base_table1和物化视图的分区是一比一映射的，base_table1有一个分区，物化视图就有对应的分区
create materialized view mydb.mv_partition_one_to_one
partition by dt
distributed by hash(id) buckets 1
refresh async
as
select
    t1.dt,
    t1.id,
    t1.name,
    t1.age,
    t1.sex,
    t2.name sex_name,
    now() ts
from mydb.base_table1 t1,mydb.dim_table_sex t2
where t1.sex=t2.id;

# 本章内容：异步物化视图的刷新
# 如果是关联分区的基表的分区有改变，那么物化视图中只会刷新该分区的数据。例如base_table1的p20240731分区产生了改变，那么就只会用p20240731的数据和dim_table_sex表进行关联，将关联结果重新存储到物化视图的p20240731分区中。
# 如果是没有关联分区的基表发生了改变，那么物化视图中会刷新所有分区的数据。例如dim_table_sex产生了修改，那么会将base_table1所有分区的数据和dim_table_sex表进行关联，将关联结果重新存储到物化视图中。
# 1.查询物化视图当前的数据
# +----------+--+-----------+---+---+--------+---------------------+
# |dt        |id|name       |age|sex|sex_name|ts                   |
# +----------+--+-----------+---+---+--------+---------------------+
# |2024-08-02|1 |zhang zhang|20 |1  |男       |2024-08-02 08:09:43.0|
# |2024-08-01|2 |li si      |30 |2  |male    |2024-08-02 07:42:54.0|
# |2024-07-30|3 |wang wu    |35 |3  |male1   |2024-08-02 09:35:48.0|
# |2024-07-30|10|zhao liu   |22 |4  |male2   |2024-08-02 09:35:48.0|
# |2024-07-30|20|de gang    |33 |1  |男       |2024-08-02 09:35:48.0|
# +----------+--+-----------+---+---+--------+---------------------+
select * from mydb.mv_partition_one_to_one;
# 2.向关联分区的基表中插入数据，修改2024-07-30分区
insert into mydb.base_table1 values('2024-07-30',6,'liu wu',23,1);
# 3.再次查询物化视图，发现只有7月30日数据的ts变了，说明只有基表base_table1中7月30日分区被刷新了，其他分区由于没有变化，导致不会被刷新。
# +----------+--+-----------+---+---+--------+---------------------+
# |dt        |id|name       |age|sex|sex_name|ts                   |
# +----------+--+-----------+---+---+--------+---------------------+
# |2024-08-01|2 |li si      |30 |2  |male    |2024-08-02 07:42:54.0|
# |2024-08-02|1 |zhang zhang|20 |1  |男       |2024-08-02 08:09:43.0|
# |2024-07-30|3 |wang wu    |35 |3  |male1   |2024-08-02 10:14:02.0|
# |2024-07-30|6 |liu wu     |23 |1  |男       |2024-08-02 10:14:02.0|
# |2024-07-30|10|zhao liu   |22 |4  |male2   |2024-08-02 10:14:02.0|
# |2024-07-30|20|de gang    |33 |1  |男       |2024-08-02 10:14:02.0|
# +----------+--+-----------+---+---+--------+---------------------+
select * from mydb.mv_partition_one_to_one;
# 4.向非关联分区的表添加数据
insert into mydb.dim_table_sex values(50,'女');
# 5.再查询物化视图，发现所有分区的数据中的ts都改变了，说明刷新了关联分区的基表的所有分区
# +----------+--+-----------+---+---+--------+---------------------+
# |dt        |id|name       |age|sex|sex_name|ts                   |
# +----------+--+-----------+---+---+--------+---------------------+
# |2024-08-02|1 |zhang zhang|20 |1  |男       |2024-08-02 10:20:14.0|
# |2024-08-01|2 |li si      |30 |2  |male    |2024-08-02 10:20:14.0|
# |2024-07-31|5 |liu liu    |20 |1  |男       |2024-08-02 10:20:14.0|
# |2024-07-30|3 |wang wu    |35 |3  |male1   |2024-08-02 10:20:14.0|
# |2024-07-30|6 |liu wu     |23 |1  |男       |2024-08-02 10:20:14.0|
# |2024-07-30|10|zhao liu   |22 |4  |male2   |2024-08-02 10:20:14.0|
# |2024-07-30|20|de gang    |33 |1  |男       |2024-08-02 10:20:14.0|
# +----------+--+-----------+---+---+--------+---------------------+
select * from mydb.mv_partition_one_to_one;

# 本章内容：auto_refresh_partitions_limit参数
# 可以通过auto_refresh_partitions_limit参数，降低每次刷新的分区数量，从而减少分区刷新的开销，但代价是物化视图和基表可能不一致。
# 如果没配置auto_refresh_partitions_limit，当dim_table_sex发生改变后，base_table1中所有分区(p20240728、p20240729、p20240730、p20240731)都会参与物化视图的刷新，最终影响物化视图的所有分区。
# 但是如果将auto_refresh_partitions_limit设置为3，假设当前日期为2024-07-31，那么就只有base_table1中的p20240729、p20240730、p20240731这三个分区参与刷新，最终只会刷新物化视图的p20240729、p20240730、p20240731这三个分区。
# 可以看到p20240728分区没有参与刷新，减少了刷新的工作量，但代价是物化视图中p20240728分区的数据可能和base_table1的p20240728分区中的数据不一致。
# 1.修改物化视图，增加auto_refresh_partitions_limit参数
ALTER MATERIALIZED VIEW mydb.mv_partition_one_to_one SET ("auto_refresh_partitions_limit" = "2");
-- 2.改变非关联分区的基表，理论上会导致关联分区的基表base_table1中所有分区都参与刷新，最终影响物化视图的所有分区。但由于配置了auto_refresh_partitions_limit=2，只有base_table1中p20240729,p20240730分区参与刷新（当前日期为2024-07-30），最终只影响物化视图中的p20240729,p20240730这两个分区。
insert into mydb.dim_table_sex values(5,'male3');
-- 3.查询物化视图，只有p20240729,p20240730分区的数据中的ts字段改变了，说明只有这两个分区被重新刷新了
-- 查询到的物化视图中数据如下，可以看到dt=2024-07-29、2024-07-30这两个分区的数据中的ts被修改了
# +----------+--+---------+---+---+--------+---------------------+
# |dt        |id|name     |age|sex|sex_name|ts                   |
# +----------+--+---------+---+---+--------+---------------------+
# |2024-07-28|3 |wang wu  |35 |3  |male1   |2024-07-31 07:55:46.0|
# |2024-07-29|2 |li si    |30 |2  |male    |2024-07-31 08:35:31.0|
# |2024-07-30|1 |zhang san|20 |1  |男       |2024-07-31 08:35:31.0|
# +----------+--+---------+---+---+--------+---------------------+
select * from mydb.mv_partition_one_to_one;
-- 4.将auto_refresh_partitions_limit改回默认值-1，也就是不限制参与刷新的分区数量
ALTER MATERIALIZED VIEW mydb.mv_partition_one_to_one SET ("auto_refresh_partitions_limit" = "-1");
-- 5.修改非关联分区的基表
insert into mydb.dim_table_sex values(5,'male3');
-- 6.再查询物化视图，发现所有分区的数据的ts字段都被修改了，说明物化视图中所有分区都被刷新了
# +----------+--+---------+---+---+--------+---------------------+
# |dt        |id|name     |age|sex|sex_name|ts                   |
# +----------+--+---------+---+---+--------+---------------------+
# |2024-07-28|3 |wang wu  |35 |3  |male1   |2024-07-31 08:46:24.0|
# |2024-07-29|2 |li si    |30 |2  |male    |2024-07-31 08:46:24.0|
# |2024-07-30|1 |zhang san|20 |1  |男       |2024-07-31 08:46:24.0|
# +----------+--+---------+---+---+--------+---------------------+
select * from mydb.mv_partition_one_to_one;

# 本章内容：物化视图查询以及物化视图的执行任务查询
# 1. 查询所有异步物化视图。可以查询到视图的状态以及最近一次任务的执行情况
# [
#   {
#     "MATERIALIZED_VIEW_ID": "14163",
#     "TABLE_SCHEMA": "mydb",
#     "TABLE_NAME": "mv_partition_one_to_one",
#     "REFRESH_TYPE": "ASYNC",
#     "IS_ACTIVE": "true",
#     "INACTIVE_REASON": "",
#     "PARTITION_TYPE": "RANGE",
#     "TASK_ID": "14165",
#     "TASK_NAME": "mv-14163",
#     "LAST_REFRESH_START_TIME": "2024-07-31 12:40:16",
#     "LAST_REFRESH_FINISHED_TIME": "2024-07-31 12:40:17",
#     "LAST_REFRESH_DURATION": "1.204",
#     "LAST_REFRESH_STATE": "SUCCESS",
#     "LAST_REFRESH_FORCE_REFRESH": "false",
#     "LAST_REFRESH_START_PARTITION": "",
#     "LAST_REFRESH_END_PARTITION": "",
#     "LAST_REFRESH_BASE_REFRESH_PARTITIONS": "{dim_table_sex=[dim_table_sex], base_table1=[p20240730]}",
#     "LAST_REFRESH_MV_REFRESH_PARTITIONS": "p20240730",
#     "LAST_REFRESH_ERROR_CODE": "0",
#     "LAST_REFRESH_ERROR_MESSAGE": "",
#     "TABLE_ROWS": "4",
#     "MATERIALIZED_VIEW_DEFINITION": "CREATE MATERIALIZED VIEW `mv_partition_one_to_one` (`dt`, `id`, `name`, `age`, `sex`, `sex_name`, `ts`)\nPARTITION BY (`dt`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 1 \nREFRESH ASYNC\nPROPERTIES (\n\"auto_refresh_partitions_limit\" = \"-1\",\n\"replicated_storage\" = \"true\",\n\"replication_num\" = \"1\",\n\"storage_medium\" = \"HDD\"\n)\nAS SELECT `t1`.`dt`, `t1`.`id`, `t1`.`name`, `t1`.`age`, `t1`.`sex`, `t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `t1`.`sex` = `t2`.`id`;"
#   },
#   {
#     "MATERIALIZED_VIEW_ID": "14266",
#     "TABLE_SCHEMA": "mydb",
#     "TABLE_NAME": "mv_refresh_period",
#     "REFRESH_TYPE": "ASYNC",
#     "IS_ACTIVE": "true",
#     "INACTIVE_REASON": "",
#     "PARTITION_TYPE": "RANGE",
#     "TASK_ID": "14268",
#     "TASK_NAME": "mv-14266",
#     "LAST_REFRESH_START_TIME": "2024-07-31 12:51:19",
#     "LAST_REFRESH_FINISHED_TIME": "2024-07-31 12:51:20",
#     "LAST_REFRESH_DURATION": "1.007",
#     "LAST_REFRESH_STATE": "SUCCESS",
#     "LAST_REFRESH_FORCE_REFRESH": "false",
#     "LAST_REFRESH_START_PARTITION": "",
#     "LAST_REFRESH_END_PARTITION": "",
#     "LAST_REFRESH_BASE_REFRESH_PARTITIONS": "{}",
#     "LAST_REFRESH_MV_REFRESH_PARTITIONS": "",
#     "LAST_REFRESH_ERROR_CODE": "0",
#     "LAST_REFRESH_ERROR_MESSAGE": "",
#     "TABLE_ROWS": "4",
#     "MATERIALIZED_VIEW_DEFINITION": "CREATE MATERIALIZED VIEW `mv_refresh_period` (`dt`, `id`, `name`, `age`, `sex`, `sex_name`, `ts`)\nPARTITION BY (`dt`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 1 \nREFRESH ASYNC EVERY(INTERVAL 1 MINUTE)\nPROPERTIES (\n\"replicated_storage\" = \"true\",\n\"replication_num\" = \"1\",\n\"storage_medium\" = \"HDD\"\n)\nAS SELECT `t1`.`dt`, `t1`.`id`, `t1`.`name`, `t1`.`age`, `t1`.`sex`, `t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `t1`.`sex` = `t2`.`id`;"
#   }
# ]
select * from information_schema.materialized_views;
# 2. 可以拿着从information_schema.materialized_views的task_name字段的值，来information_schema.tasks表查询该物化视图对应的task信息
# [
#   {
#     "TASK_NAME": "mv-14045",
#     "CREATE_TIME": "2024-07-30 12:42:29",
#     "SCHEDULE": "EVENT_TRIGGERED",
#     "DATABASE": "mydb",
#     "DEFINITION": "insert overwrite `dwd_view` SELECT `mydb`.`t1`.`dt`, `mydb`.`t1`.`id`, `mydb`.`t1`.`name`, `mydb`.`t1`.`age`, `mydb`.`t1`.`sex`, `mydb`.`t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `mydb`.`t1`.`sex` = `mydb`.`t2`.`id`",
#     "EXPIRE_TIME": null
#   }
# ]
select * from information_schema.tasks where task_name='mv-14045';
# 3. 一个task会在每次执行时生成一个task_run，可以拿着task_name去information_schema.tasks表task对应的task_run信息
# [
#   {
#     "QUERY_ID": "537d827d-4e71-11ef-8296-0242ac120003",
#     "TASK_NAME": "mv-14045",
#     "CREATE_TIME": "2024-07-30 12:43:31",
#     "FINISH_TIME": "2024-07-30 12:43:32",
#     "STATE": "SUCCESS",
#     "DATABASE": "mydb",
#     "DEFINITION": "insert overwrite `dwd_view` SELECT `mydb`.`t1`.`dt`, `mydb`.`t1`.`id`, `mydb`.`t1`.`name`, `mydb`.`t1`.`age`, `mydb`.`t1`.`sex`, `mydb`.`t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `mydb`.`t1`.`sex` = `mydb`.`t2`.`id`",
#     "EXPIRE_TIME": "2024-07-31 12:43:31",
#     "ERROR_CODE": 0,
#     "ERROR_MESSAGE": null,
#     "PROGRESS": "100%",
#     "EXTRA_MESSAGE": "{\"forceRefresh\":false,\"mvPartitionsToRefresh\":[\"p20240730\"],\"refBasePartitionsToRefreshMap\":{\"base_table1\":[\"p20240730\"]},\"basePartitionsToRefreshMap\":{\"dim_table_sex\":[\"dim_table_sex\"],\"base_table1\":[\"p20240730\"]}}",
#     "PROPERTIES": "{\"FORCE\":\"false\"}"
#   },
#   {
#     "QUERY_ID": "2ea37608-4e71-11ef-8296-0242ac120003",
#     "TASK_NAME": "mv-14045",
#     "CREATE_TIME": "2024-07-30 12:42:29",
#     "FINISH_TIME": "2024-07-30 12:42:30",
#     "STATE": "SUCCESS",
#     "DATABASE": "mydb",
#     "DEFINITION": "insert overwrite `dwd_view` SELECT `mydb`.`t1`.`dt`, `mydb`.`t1`.`id`, `mydb`.`t1`.`name`, `mydb`.`t1`.`age`, `mydb`.`t1`.`sex`, `mydb`.`t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `mydb`.`t1`.`sex` = `mydb`.`t2`.`id`",
#     "EXPIRE_TIME": "2024-07-31 12:42:29",
#     "ERROR_CODE": 0,
#     "ERROR_MESSAGE": null,
#     "PROGRESS": "100%",
#     "EXTRA_MESSAGE": "{\"forceRefresh\":false,\"mvPartitionsToRefresh\":[\"p20240730\"],\"refBasePartitionsToRefreshMap\":{\"base_table1\":[\"p20240730\"]},\"basePartitionsToRefreshMap\":{\"dim_table_sex\":[\"dim_table_sex\"],\"base_table1\":[\"p20240730\"]}}",
#     "PROPERTIES": ""
#   }
# ]
select * from information_schema.task_runs where task_name='mv-14477';
# 4.通过task_runs表可以查询到query_id，我们可以使用该字段的值来loads表中查询到本次load任务的细节
# [
#   {
#     "JOB_ID": 14756,
#     "LABEL": "insert_d45021c8-50a2-11ef-8296-0242ac120003",
#     "DATABASE_NAME": "mydb",
#     "STATE": "FINISHED",
#     "PROGRESS": "ETL:100%; LOAD:100%",
#     "TYPE": "INSERT",
#     "PRIORITY": "NORMAL",
#     "SCAN_ROWS": 4,
#     "FILTERED_ROWS": 0,
#     "UNSELECTED_ROWS": 0,
#     "SINK_ROWS": 2,
#     "ETL_INFO": "",
#     "TASK_INFO": "resource:N/A; timeout(s):3600; max_filter_ratio:0.0",
#     "CREATE_TIME": "2024-08-02 07:42:54",
#     "ETL_START_TIME": "2024-08-02 07:42:54",
#     "ETL_FINISH_TIME": "2024-08-02 07:42:54",
#     "LOAD_START_TIME": "2024-08-02 07:42:54",
#     "LOAD_FINISH_TIME": "2024-08-02 07:42:55",
#     "JOB_DETAILS": "{\"All backends\":{\"d45021c8-50a2-11ef-8296-0242ac120003\":[10004]},\"FileNumber\":0,\"FileSize\":0,\"InternalTableLoadBytes\":93,\"InternalTableLoadRows\":2,\"ScanBytes\":38,\"ScanRows\":4,\"TaskNumber\":1,\"Unfinished backends\":{\"d45021c8-50a2-11ef-8296-0242ac120003\":[]}}",
#     "ERROR_MSG": null,
#     "TRACKING_URL": null,
#     "TRACKING_SQL": null,
#     "REJECTED_RECORD_PATH": null
#   }
# ]
select * from information_schema.loads where label like '%d45021c8-50a2-11ef-8296-0242ac120003';

# 本章内容：创建分区上卷的物化视图
# 1.创建一个分区上卷的物化视图，也是用一个基表的分区来关联物化视图的分区，但是这里就可以不是基表有一个分区，物化视图就也有一个分区，而是基表的多个分区，对应物化视图的一个分区
# 假设基表的多个分区p20240728、p20240729、p20240730对应物化视图的一个分区p202407。在视图刷新时，当基表的p20240728分区发生变化，那么物化视图中p202407对应的基表的所有分区(p20240728、p20240729、p20240730)都会参与刷新，来更新到物化视图p202407的分区中
# 什么时候适合用上卷的物化视图？
# 当基表一个分区改变时，希望把一定范围的分区也重新刷新，此时就可以使用上卷的物化视图。
create materialized view mydb.mv_partition_rollup
partition by date_trunc('month',dt)
distributed by hash(sex_name) buckets 1
refresh async
as
select
    dt,
    sex_name,
    count(*) cnt,
    now() ts
from (
  select
      t1.dt,
      t1.id,
      t1.name,
      t1.age,
      t1.sex,
      t2.name sex_name,
      now() ts
  from mydb.base_table1 t1,mydb.dim_table_sex t2
  where t1.sex=t2.id
) t
group by
    dt,sex_name;
# 2.查询物化视图的分区，发现是以月为分区，而不是基表的以天为分区
# +-----------+--------------+--------------+-------------------+------------------+------+------------+--------------------------------------------------------------------------+---------------+-------+--------------+-------------+-------------------+------------------------+--------+----------+--------+
# |PartitionId|PartitionName |VisibleVersion|VisibleVersionTime |VisibleVersionHash|State |PartitionKey|Range                                                                     |DistributionKey|Buckets|ReplicationNum|StorageMedium|CooldownTime       |LastConsistencyCheckTime|DataSize|IsInMemory|RowCount|
# +-----------+--------------+--------------+-------------------+------------------+------+------------+--------------------------------------------------------------------------+---------------+-------+--------------+-------------+-------------------+------------------------+--------+----------+--------+
# |14848      |p202407_202408|2             |2024-08-02 09:35:48|0                 |NORMAL|dt          |[types: [DATE]; keys: [2024-07-01]; ..types: [DATE]; keys: [2024-08-01]; )|sex_name       |1      |1             |HDD          |9999-12-31 15:59:59|null                    |1020B   |false     |3       |
# |14826      |p202408_202409|2             |2024-08-02 08:09:43|0                 |NORMAL|dt          |[types: [DATE]; keys: [2024-08-01]; ..types: [DATE]; keys: [2024-09-01]; )|sex_name       |1      |1             |HDD          |9999-12-31 15:59:59|null                    |1007B   |false     |2       |
# +-----------+--------------+--------------+-------------------+------------------+------+------------+--------------------------------------------------------------------------+---------------+-------+--------------+-------------+-------------------+------------------------+--------+----------+--------+
show partitions from mydb.mv_partition_rollup;
# 3.查询视图中数据内容
# +----------+--------+---+---------------------+
# |dt        |sex_name|cnt|ts                   |
# +----------+--------+---+---------------------+
# |2024-08-01|male    |1  |2024-08-02 08:09:43.0|
# |2024-08-02|男       |1  |2024-08-02 08:09:43.0|
# |2024-07-30|male1   |1  |2024-08-02 07:55:05.0|
# |2024-07-30|male2   |1  |2024-08-02 07:55:05.0|
# +----------+--------+---+---------------------+
select * from mydb.mv_partition_rollup;
# 4.修改基表中7月份的某一个分区的数据
insert into mydb.base_table1 values('2024-07-30',20,'de gang',33,1);
# 5.发现视图中7月份的数据的ts都改变了，说明基表base_table1中7月份的分区都重新刷新了，将刷新结果存储到物化视图的7月份的分区
# +----------+--------+---+---------------------+
# |dt        |sex_name|cnt|ts                   |
# +----------+--------+---+---------------------+
# |2024-07-30|male1   |1  |2024-08-02 09:35:48.0|
# |2024-07-30|male2   |1  |2024-08-02 09:35:48.0|
# |2024-07-30|男       |1  |2024-08-02 09:35:48.0|
# |2024-08-01|male    |1  |2024-08-02 08:09:43.0|
# |2024-08-02|男       |1  |2024-08-02 08:09:43.0|
# +----------+--------+---+---------------------+
select * from mydb.mv_partition_rollup;

# 本章内容：基表的重建、以及物化视图的重新置为有效
# 1.删除基表，查看异步物化视图的状态
drop table mydb.base_table1;
# 2.当基表被删除，异步物化视图变为inactive状态
# +--------------------+------------+-------------------+------------+---------+-------------------------------+--------------+-------+---------+-----------------------+--------------------------+---------------------+------------------+--------------------------+----------------------------+--------------------------+------------------------------------------------------------------------------+----------------------------------+-----------------------+--------------------------+----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |MATERIALIZED_VIEW_ID|TABLE_SCHEMA|TABLE_NAME         |REFRESH_TYPE|IS_ACTIVE|INACTIVE_REASON                |PARTITION_TYPE|TASK_ID|TASK_NAME|LAST_REFRESH_START_TIME|LAST_REFRESH_FINISHED_TIME|LAST_REFRESH_DURATION|LAST_REFRESH_STATE|LAST_REFRESH_FORCE_REFRESH|LAST_REFRESH_START_PARTITION|LAST_REFRESH_END_PARTITION|LAST_REFRESH_BASE_REFRESH_PARTITIONS                                          |LAST_REFRESH_MV_REFRESH_PARTITIONS|LAST_REFRESH_ERROR_CODE|LAST_REFRESH_ERROR_MESSAGE|TABLE_ROWS|MATERIALIZED_VIEW_DEFINITION                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
# +--------------------+------------+-------------------+------------+---------+-------------------------------+--------------+-------+---------+-----------------------+--------------------------+---------------------+------------------+--------------------------+----------------------------+--------------------------+------------------------------------------------------------------------------+----------------------------------+-----------------------+--------------------------+----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |14477               |mydb        |mv_partition_rollup|ASYNC       |false    |base-table dropped: base_table1|RANGE         |14479  |mv-14477 |2024-08-02 02:58:44    |2024-08-02 02:58:45       |1.250                |SUCCESS           |false                     |                            |                          |{dim_table_sex=[dim_table_sex], base_table1=[p20240617, p20240618, p20240615]}|p202406_202407                    |0                      |                          |10        |CREATE MATERIALIZED VIEW `mv_partition_rollup` (`dt`, `sex_name`, `cnt`, `ts`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |PARTITION BY (date_trunc('month', `dt`))                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |DISTRIBUTED BY HASH(`sex_name`) BUCKETS 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |REFRESH ASYNC                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |PROPERTIES (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |"replicated_storage" = "true",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |"replication_num" = "1",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |"storage_medium" = "HDD"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |AS SELECT `t`.`dt`, `t`.`sex_name`, count(*) AS `cnt`, now() AS `ts`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |FROM (SELECT `t1`.`dt`, `t1`.`id`, `t1`.`name`, `t1`.`age`, `t1`.`sex`, `t2`.`name` AS `sex_name`, now() AS `ts`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |FROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |WHERE `t1`.`sex` = `t2`.`id`) `t`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
# |                    |            |                   |            |         |                               |              |       |         |                       |                          |                     |                  |                          |                            |                          |                                                                              |                                  |                       |                          |          |GROUP BY `t`.`dt`, `t`.`sex_name`;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
# +--------------------+------------+-------------------+------------+---------+-------------------------------+--------------+-------+---------+-----------------------+--------------------------+---------------------+------------------+--------------------------+----------------------------+--------------------------+------------------------------------------------------------------------------+----------------------------------+-----------------------+--------------------------+----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
select * from information_schema.materialized_views where table_name='mv_partition_rollup';
# 3.尽管异步物化视图变为inactive状态，但仍然可以查询出异步物化视图中已有的数据，只是无法用该异步物化视图进行sql改写了
select * from mydb.mv_partition_rollup;
# 4.此时如果触发异步物化视图的刷新，会发现task_runs中显示任务执行失败
# [
#   {
#     "QUERY_ID": "3a2773db-507e-11ef-8296-0242ac120003",
#     "TASK_NAME": "mv-14477",
#     "CREATE_TIME": "2024-08-02 03:20:54",
#     "FINISH_TIME": "2024-08-02 03:20:54",
#     "STATE": "FAILED",
#     "DATABASE": "mydb",
#     "DEFINITION": "insert overwrite `mv_partition_rollup` SELECT `t`.`dt`, `t`.`sex_name`, count(*) AS `cnt`, now() AS `ts`\nFROM (SELECT `mydb`.`t1`.`dt`, `mydb`.`t1`.`id`, `mydb`.`t1`.`name`, `mydb`.`t1`.`age`, `mydb`.`t1`.`sex`, `mydb`.`t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `mydb`.`t1`.`sex` = `mydb`.`t2`.`id`) `t`\nGROUP BY `t`.`dt`, `t`.`sex_name`",
#     "EXPIRE_TIME": "2024-08-03 03:20:54",
#     "ERROR_CODE": -1,
#     "ERROR_MESSAGE": "com.starrocks.sql.common.DmlException: Materialized view: mv_partition_rollup, id: 14477 is not active, skip sync partition and data with base tables",
#     "PROGRESS": "0%",
#     "EXTRA_MESSAGE": "{\"forceRefresh\":false,\"mvPartitionsToRefresh\":[],\"refBasePartitionsToRefreshMap\":{},\"basePartitionsToRefreshMap\":{}}",
#     "PROPERTIES": "{\"FORCE\":\"false\"}"
#   }
# ]
select * from information_schema.task_runs where task_name='mv-14477';
# 5.重新建相同名字的基表mydb.base_table1后，发现异步物化视图依然是inactive状态
create table mydb.base_table1(
                                 dt date,
                                 id int,
                                 name varchar(50),
                                 age int,
                                 sex int
)
    primary key(dt,id)
partition by date_trunc('day',dt)
distributed by hash(id);
# 6.由于被删除的基表已经重建了，因此手动将异步物化视图的状态置为active
ALTER MATERIALIZED VIEW mydb.mv_partition_rollup ACTIVE;
# 7.确认视图的状态是否真变为active了
# +---------+
# |is_active|
# +---------+
# |true     |
# +---------+
select is_active from information_schema.materialized_views where table_name='mv_partition_rollup';
# 8.手动刷新异步物化视图
refresh MATERIALIZED VIEW mydb.mv_partition_rollup;
# 9.发现基表重新建立，且将物化视图状态手动设置为active后，异步物化视图的任务就可以正常执行了
# [
#   {
#     "QUERY_ID": "698eceec-507f-11ef-8296-0242ac120003",
#     "TASK_NAME": "mv-14477",
#     "CREATE_TIME": "2024-08-02 03:29:23",
#     "FINISH_TIME": "2024-08-02 03:29:23",
#     "STATE": "SUCCESS",
#     "DATABASE": "mydb",
#     "DEFINITION": "insert overwrite `mv_partition_rollup` SELECT `t`.`dt`, `t`.`sex_name`, count(*) AS `cnt`, now() AS `ts`\nFROM (SELECT `mydb`.`t1`.`dt`, `mydb`.`t1`.`id`, `mydb`.`t1`.`name`, `mydb`.`t1`.`age`, `mydb`.`t1`.`sex`, `mydb`.`t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `mydb`.`t1`.`sex` = `mydb`.`t2`.`id`) `t`\nGROUP BY `t`.`dt`, `t`.`sex_name`",
#     "EXPIRE_TIME": "2024-08-03 03:29:23",
#     "ERROR_CODE": 0,
#     "ERROR_MESSAGE": null,
#     "PROGRESS": "100%",
#     "EXTRA_MESSAGE": "{\"forceRefresh\":false,\"mvPartitionsToRefresh\":[],\"refBasePartitionsToRefreshMap\":{},\"basePartitionsToRefreshMap\":{}}",
#     "PROPERTIES": "{\"FORCE\":\"false\"}"
#   },
#   {
#     "QUERY_ID": "cb8ef64e-507e-11ef-8296-0242ac120003",
#     "TASK_NAME": "mv-14477",
#     "CREATE_TIME": "2024-08-02 03:24:58",
#     "FINISH_TIME": "2024-08-02 03:24:58",
#     "STATE": "SUCCESS",
#     "DATABASE": "mydb",
#     "DEFINITION": "insert overwrite `mv_partition_rollup` SELECT `t`.`dt`, `t`.`sex_name`, count(*) AS `cnt`, now() AS `ts`\nFROM (SELECT `mydb`.`t1`.`dt`, `mydb`.`t1`.`id`, `mydb`.`t1`.`name`, `mydb`.`t1`.`age`, `mydb`.`t1`.`sex`, `mydb`.`t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `mydb`.`t1`.`sex` = `mydb`.`t2`.`id`) `t`\nGROUP BY `t`.`dt`, `t`.`sex_name`",
#     "EXPIRE_TIME": "2024-08-03 03:24:58",
#     "ERROR_CODE": 0,
#     "ERROR_MESSAGE": null,
#     "PROGRESS": "100%",
#     "EXTRA_MESSAGE": "{\"forceRefresh\":true,\"mvPartitionsToRefresh\":[],\"refBasePartitionsToRefreshMap\":{},\"basePartitionsToRefreshMap\":{}}",
#     "PROPERTIES": "{\"FORCE\":\"true\"}"
#   },
#   {
#     "QUERY_ID": "3a2773db-507e-11ef-8296-0242ac120003",
#     "TASK_NAME": "mv-14477",
#     "CREATE_TIME": "2024-08-02 03:20:54",
#     "FINISH_TIME": "2024-08-02 03:20:54",
#     "STATE": "FAILED",
#     "DATABASE": "mydb",
#     "DEFINITION": "insert overwrite `mv_partition_rollup` SELECT `t`.`dt`, `t`.`sex_name`, count(*) AS `cnt`, now() AS `ts`\nFROM (SELECT `mydb`.`t1`.`dt`, `mydb`.`t1`.`id`, `mydb`.`t1`.`name`, `mydb`.`t1`.`age`, `mydb`.`t1`.`sex`, `mydb`.`t2`.`name` AS `sex_name`, now() AS `ts`\nFROM `mydb`.`base_table1` AS `t1` , `mydb`.`dim_table_sex` AS `t2` \nWHERE `mydb`.`t1`.`sex` = `mydb`.`t2`.`id`) `t`\nGROUP BY `t`.`dt`, `t`.`sex_name`",
#     "EXPIRE_TIME": "2024-08-03 03:20:54",
#     "ERROR_CODE": -1,
#     "ERROR_MESSAGE": "com.starrocks.sql.common.DmlException: Materialized view: mv_partition_rollup, id: 14477 is not active, skip sync partition and data with base tables",
#     "PROGRESS": "0%",
#     "EXTRA_MESSAGE": "{\"forceRefresh\":false,\"mvPartitionsToRefresh\":[],\"refBasePartitionsToRefreshMap\":{},\"basePartitionsToRefreshMap\":{}}",
#     "PROPERTIES": "{\"FORCE\":\"false\"}"
#   }
# ]
select * from information_schema.task_runs where task_name='mv-14477';

# 本章内容：检查异步物化视图任务的资源消耗
# 1.修改全局变量参数，将enable_profile变量设置为true，默认是false。（注意：这里要设置全局参数，官方文档上写的是设置session变量，但我设置了不起作用，异步物化视图刷新后，通过show profilelist找不到该query id）
set global enable_profile=true;
# 注意：谨慎启用enable_profile，因为抓取sql执行过程本身也很吃sr集群资源。生产环境建议将enable_profile设置为false，通过big_query_profile_threshold参数，仅针对慢查询才启动profile。这样既保证了系统性能，又能有效监控到慢查询。
# SET global big_query_profile_threshold = '30s';
# big_query_profile_threshold参数描述：用于设定大查询的阈值。当会话变量 enable_profile 设置为 false 且查询时间超过 big_query_profile_threshold 设定的阈值时，则会生成 Profile。
# 2.从task_runs中找到异步物化视图的执行任务
select query_id,CREATE_TIME from information_schema.task_runs where task_name='mv-14477';
# 3.通过analyze profile获取指定query id的执行记录，主要从summary中获取任务的执行耗时和内存消耗等情况
# +--------------------------------------------------------------------------------------------------------------------------------------------------------+
# |Explain String                                                                                                                                          |
# +--------------------------------------------------------------------------------------------------------------------------------------------------------+
# |Summary                                                                                                                                                 |
# |    Attention: The transaction of the statement will be aborted, and no data will be actually inserted!!!                                               |
# |    QueryId: 92d0dfb0-50a6-11ef-8296-0242ac120003                                                                                                       |
# |    Version: 3.2.2-269e832                                                                                                                              |
# |    State: Finished                                                                                                                                     |
# |    TotalTime: 471ms                                                                                                                                    |
# |        ExecutionTime: 35.661ms [Scan: 1.172ms (3.29%), Network: 1.012ms (2.84%), ResultDeliverTime: 18.153ms (50.90%), ScheduleTime: 873.913us (2.45%)]|
# |        CollectProfileTime: 0                                                                                                                           |
# |        FrontendProfileMergeTime: 11.279ms                                                                                                              |
# |    QueryPeakMemoryUsage: 1.682 MB, QueryAllocatedMemoryUsage: 1.186 MB                                                                                 |
# |    Top Most Time-consuming Nodes:                                                                                                                      |
# |        1. OLAP_SCAN (id=1) : 2.470ms (36.44%)                                                                                                          |
# |        2. OLAP_TABLE_SINK: 1.765ms (26.03%)                                                                                                            |
# |        3. OLAP_SCAN (id=0) : 974.197us (14.37%)                                                                                                        |
# |        4. EXCHANGE (id=6) [SHUFFLE]: 639.688us (9.43%)                                                                                                 |
# |        5. EXCHANGE (id=2) [SHUFFLE]: 611.831us (9.02%)                                                                                                 |
# |        6. HASH_JOIN (id=3) [BUCKET_SHUFFLE, INNER JOIN]: 125.620us (1.85%)                                                                             |
# |        7. AGGREGATION (id=5) [serialize, update]: 93.240us (1.38%)                                                                                     |
# |        8. AGGREGATION (id=7) [finalize, merge]: 73.586us (1.09%)                                                                                       |
# |        9. PROJECT (id=8) : 12.607us (0.19%)                                                                                                            |
# |        10. DECODE (id=9) : 7.286us (0.11%)                                                                                                             |
# |    Top Most Memory-consuming Nodes:                                                                                                                    |
# |        1. OLAP_SCAN (id=1) : 195.461 KB                                                                                                                |
# |        2. OLAP_SCAN (id=0) : 152.250 KB                                                                                                                |
# |        3. HASH_JOIN (id=3) [BUCKET_SHUFFLE, INNER JOIN]: 98.617 KB                                                                                     |
# |        4. AGGREGATION (id=7) [finalize, merge]: 68.242 KB                                                                                              |
# |        5. AGGREGATION (id=5) [serialize, update]: 68.241 KB                                                                                            |
# |        6. EXCHANGE (id=6) [SHUFFLE]: 6.186 KB                                                                                                          |
# |        7. EXCHANGE (id=2) [SHUFFLE]: 5.116 KB                                                                                                          |
# |    NonDefaultVariables:                                                                                                                                |
# |        big_query_log_scan_rows_threshold: 1000000000 -> 1                                                                                              |
# |        enable_adaptive_sink_dop: false -> true                                                                                                         |
# |        enable_insert_strict: true -> false                                                                                                             |
# |        enable_materialized_view_rewrite: true -> false                                                                                                 |
# |        enable_profile: false -> true                                                                                                                   |
# |        enable_spill: false -> true                                                                                                                     |
# |        query_timeout: 300 -> 3600                                                                                                                      |
# |        resource_group:  -> default_mv_wg                                                                                                               |
# |        tx_visible_wait_timeout: 10 -> 9223372036854775                                                                                                 |
# |        use_compute_nodes: -1 -> 0                                                                                                                      |
# +--------------------------------------------------------------------------------------------------------------------------------------------------------+
analyze profile from '92d0dfb0-50a6-11ef-8296-0242ac120003';
# 4.也可以通过show profilelist，找到所有异步物化视图任务的query id
# +------------------------------------+-------------------+-----+--------+--------------------------------------------------------------------------------------------------------------------------------+
# |QueryId                             |StartTime          |Time |State   |Statement                                                                                                                       |
# +------------------------------------+-------------------+-----+--------+--------------------------------------------------------------------------------------------------------------------------------+
# |9b8ce4bf-50a6-11ef-8296-0242ac120003|2024-08-02 08:09:57|14ms |Finished|/* ApplicationName=IntelliJ IDEA 2024.1.2 */ select database()                                                                  |
# |9b72a5f9-50a6-11ef-8296-0242ac120003|2024-08-02 08:09:57|12ms |Finished|SELECT @@session.tx_isolation                                                                                                   |
# |92d06a7f-50a6-11ef-8296-0242ac120003|2024-08-02 08:09:43|522ms|Finished|insert overwrite `mv_partition_one_to_one` SELECT `mydb`.`t1`.`dt`, `mydb`.`t1`.`id`, `mydb`.`t1`.`name`, `mydb`.`t1`.`age`, ...|
# |92d0dfb0-50a6-11ef-8296-0242ac120003|2024-08-02 08:09:43|471ms|Finished|insert overwrite `mv_partition_rollup` SELECT `t`.`dt`, `t`.`sex_name`, count(*) AS `cnt`, now() AS `ts`                        |
# |                                    |                   |     |        |FROM (SELECT `mydb` ...                                                                                                         |
# +------------------------------------+-------------------+-----+--------+--------------------------------------------------------------------------------------------------------------------------------+
show profilelist;

# 本章内容：检查sql重写是否生效
# 1.确认物化视图是否生效
# +-----------------------+---------+
# |table_name             |is_active|
# +-----------------------+---------+
# |mv_partition_one_to_one|true     |
# +-----------------------+---------+
select table_name,is_active from information_schema.materialized_views where table_name='mv_partition_one_to_one';
# 2.使用物化视图的sql来查询，通过执行计划中的MaterializedView知道走的物化视图，rollup知道改写到哪个物化视图了
# 0:OlapScanNode
#      TABLE: mv_partition_one_to_one
#      PREAGGREGATION: ON
#      PREDICATES: 10: id = 1
#      partitions=4/4
#      rollup: mv_partition_one_to_one
#      tabletRatio=4/4
#      tabletList=14960,14962,14956,14958
#      cardinality=1
#      avgRowSize=39.285713
#      MaterializedView: true
explain
select
    t1.dt,
    t1.id,
    t1.name,
    t1.age,
    t1.sex,
    t2.name sex_name,
    now() ts
from mydb.base_table1 t1,mydb.dim_table_sex t2
where t1.sex=t2.id
and t1.id=1;
# 3.模拟视图失效，手动将视图设置为inactive状态
alter MATERIALIZED view mydb.mv_partition_one_to_one inactive;
# 4.确认视图已处于失效状态
# +-----------------------+---------+
# |table_name             |is_active|
# +-----------------------+---------+
# |mv_partition_one_to_one|false    |
# +-----------------------+---------+
select table_name,is_active from information_schema.materialized_views where table_name='mv_partition_one_to_one';
# 5.即使视图处于失效状态，依然可以从视图中查询出数据
# +----------+--+-----------+---+---+--------+---------------------+
# |dt        |id|name       |age|sex|sex_name|ts                   |
# +----------+--+-----------+---+---+--------+---------------------+
# |2024-08-02|1 |zhang zhang|20 |1  |男       |2024-08-07 12:46:22.0|
# |2024-08-01|2 |li si      |30 |2  |male    |2024-08-07 12:46:22.0|
# |2024-07-30|3 |wang wu    |35 |3  |male1   |2024-08-07 12:46:22.0|
# |2024-07-30|6 |liu wu     |23 |1  |男       |2024-08-07 12:46:22.0|
# |2024-07-30|10|zhao liu   |22 |4  |male2   |2024-08-07 12:46:22.0|
# |2024-07-30|20|de gang    |33 |1  |男       |2024-08-07 12:46:22.0|
# |2024-07-31|5 |liu liu    |20 |1  |男       |2024-08-07 12:46:22.0|
# +----------+--+-----------+---+---+--------+---------------------+
select * from mydb.mv_partition_one_to_one;
# 6.视图变成失效状态的主要影响：
#   a) 无法进行sql重写了。因此再次查询sql的执行计划，发现不走视图了，从基表进行查询了。
#   b) 视图无法刷新了。当视图处于inactive状态时，即使触发视图任务执行，但任务也会由于视图已失效而直接失败，无法刷新视图。
# 主要是是通过产生了shuffle证明没有走视图，rollup为基表也代表直接从基表查询，而不是查询视图
# 3:HASH JOIN
#   |  join op: INNER JOIN (BUCKET_SHUFFLE)
#   |  colocate: false, reason:
#   |  equal join conjunct: 6: id = 5: sex
#   |
#   |----2:EXCHANGE
#   |
#   0:OlapScanNode
#      TABLE: dim_table_sex
#      PREAGGREGATION: ON
#      PREDICATES: 6: id IS NOT NULL
#      partitions=1/1
#      rollup: dim_table_sex
#      tabletRatio=1/1
#      tabletList=14013
#      cardinality=9
#      avgRowSize=8.0
explain
select
    t1.dt,
    t1.id,
    t1.name,
    t1.age,
    t1.sex,
    t2.name sex_name,
    now() ts
from mydb.base_table1 t1,mydb.dim_table_sex t2
where t1.sex=t2.id
  and t1.id=1;
