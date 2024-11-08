CREATE EXTERNAL CATALOG my_paimon_catalog
PROPERTIES
(
    "type" = "paimon",
    'paimon.catalog.type'='filesystem',
    'paimon.catalog.warehouse'='hdfs://namenode:9000/my-paimon'
);

set catalog my_paimon_catalog;

show databases ;

explain analyze select * from app.paimon_pk_tbl;

SHOW RESOURCE GROUPS ALL;

CREATE RESOURCE GROUP test_group
TO (
    query_type in ('select')
) -- 创建分类器，多个分类器间用英文逗号（,）分隔。
WITH (
    "cpu_core_limit" = "2",
    "mem_limit" = "0.5",
    "big_query_cpu_second_limit"="1"
);

drop resource group test_group;

     select * from app.paimon_pk_tbl;


SHOW  processlist;
kill query 11;

show databases ;

create database mydb;

create table mydb.agg_tbl(
    id int,
    name varchar(200) replace_if_not_null,
    age int max,
    cnt int sum
)
aggregate key(id)
distributed by hash(id) buckets 2;

insert into mydb.agg_tbl values(1,'hello world',12,1);

select * from mydb.agg_tbl;

use mydb;

submit task task1 as insert into mydb.agg_tbl values(1,null,33,1);

select * from information_schema.tasks;
select * from information_schema.task_runs;

analyze profile from '3ba9debc-972f-11ef-9cf0-0242ac140002';

show processlist;

select * from information_schema.loads;