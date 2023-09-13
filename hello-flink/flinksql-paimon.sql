CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '/my-paimon'
);

use catalog paimon_catalog;

create table paimon_hello_world(
                                   id string,
                                   age int,
                                   name string,
                                    dt string,
                                    primary key (id,dt) not enforced
)
partitioned by(dt)
with(
     'merge-engine'='partial-update',
    'bucket'='3'
    -- 会产生changelog，例如先插入一条+I的数据，再插入相同key的+I数据的话，实际会产生两条数据，一条是-U，代表更新前的数据，另一条是+U，代表更新后的数据（更新的字段和未更新的字段都会有值）
--     'changelog-producer'='full-compaction'
     );

insert into paimon_hello_world values('1',10,cast(null as string),'2023-08-01');
insert into paimon_hello_world values('1',cast(null as int),'hello');
insert into paimon_hello_world values('1',30,cast(null as string));
insert into paimon_hello_world values('1',50,'world');
insert into paimon_hello_world values('1',60,'hello world');
insert into paimon_hello_world values('1',80,'hello world paimon');
insert into paimon_hello_world values('200',10,'zhang san','2023-08-02');
insert into paimon_hello_world values('300',30,'lisi','2023-08-01');
insert into paimon_hello_world values('500',99,'zhangwu','2023-08-01');
insert into paimon_hello_world values('130',99,'zhangwu','2023-08-01');
insert into paimon_hello_world values('888',99,'zhangwu','2023-08-01');
insert into paimon_hello_world values('666',99,'zhangwu','2023-08-01');
insert into paimon_hello_world values('333',99,'zhangwu','2023-08-01');
insert into paimon_hello_world values('789',99,'zhangwu','2023-08-01'),('337',99,'zhangwu','2023-08-02');
insert into paimon_hello_world values('311',99,'zhangwu','2023-08-01'),('899',99,'zhangwu','2023-08-02');



set execution.runtime-mode=batch;
delete from paimon_waybill_c where dt>='2023-09-09';

set execution.runtime-mode=streaming;
SELECT * FROM paimon_hello_world /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='1') */;
SELECT * FROM paimon_hello_world /*+ OPTIONS('scan.mode'='compacted-full') */;
SELECT * FROM paimon_hello_world$audit_log;

flink run -Dexecution.runtime-mode=batch ./lib/paimon-flink-action-0.5-20230825.002838-119.jar compact --warehouse /my-paimon --database default --table paimon_waybill_c


create table paimon_waybill_c(
                                 waybillCode string,
                                 waybillSign string,
                                 siteCode string,
                                 siteName string,
                                 `timeStamp` bigint,
                                 dt string,
                                   primary key (waybillCode,dt) not enforced
)
    partitioned by(dt)
with(
    'auto-create' = 'true',
     'merge-engine'='partial-update',
    'bucket'='1',
    -- LSM-tree中超过几个文件后，进行一次compaction
    --'num-sorted-run.compaction-trigger'='3',
    -- 经过几个cp后，进行一次full compaction
    'full-compaction.delta-commits'='5',
    -- 会产生changelog，例如先插入一条+I的数据，再插入相同key的+I数据的话，实际会产生两条数据，一条是-U，代表更新前的数据，另一条是+U，代表更新后的数据（更新的字段和未更新的字段都会有值）
    'changelog-producer'='full-compaction',
    'manifest.format'='orc',
    'snapshot.num-retained.max'='1000',
    'snapshot.num-retained.min'='1'
);

-- 如果在使用paimon catalog，那么创建的表必须是paimon的connector。但是如果我想在paimon catalog下建一个其他connector的table要怎么做呢？可以使用TEMPORARY关键字，这样可以创建其他connector的表，同时paimon catalog也不会记录该表。
-- 下次重启sql-client客户端时，paimon catalog下就看不到这个表了。
create TEMPORARY table hello_kafka(
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

set execution.checkpointing.interval='60 s';
insert into paimon_waybill_c select * from hello_kafka;

-- 选取最新的snapshot，然后持续读未来产生的change log，由于读的是change log，因此下发的数据带有-U，+U类型
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='latest') */;

-- 读最新快照，而不是change log，根据最新快照合并base file和delta file，获取完整数据。由于读的不是change log，因此下发的数据都是+I类型的。
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='latest-full') */;

-- 读取最新compact快照中的数据
-- 如果是批读，则读取最后一次compact类型的snapshot中的数据文件，如果snapshot列表中没有compact类型的snapshot，则取最后一个snapshot，也就是和latest-full一样了
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='compacted-full') */;

-- 如果是批读，则找到小于或等于该timestamp的snapshot（使用snapshot中的timeMillis字段来比较），然后读取并合并出全表的完整数据
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='from-timestamp','scan.timestamp-millis'='1694165037727') */;
-- 如果是批读，就是获取指定的snapshot，根据根据snapshot中存储的base和delta manifest-list，进行合并，将合并结果发送至下游
-- 如果是流读，则是读取指定snapshot之后的change log，注意只读change log
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='2') */;


SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='1') */;

SELECT * FROM paimon_waybill_c$snapshots;


set sql-client.execution.result-mode=TABLEAU;
SELECT siteCode,count(*) cnt FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='1') */
group by siteCode;

set execution.runtime-mode=streaming;
insert into paimon_waybill_c values('JDA','0010','11','site11',1,'2023-06-21'),('JDB','1111','15','site15',1,'2023-06-21'),('JDC','1110','11','site11',1,'2023-06-22');
insert into paimon_waybill_c values('JDB','1101','20','site20',2,'2023-06-21'),('JDC','0101','20','site20',2,'2023-06-22');
insert into paimon_waybill_c values('JDA','111100','20','site20',3,'2023-06-21'),('JDD','101010','20','site20',3,'2023-06-21'),('JDE','000000','20','site20',3,'2023-06-21'),('JDF','11','20','site20',3,'2023-06-22'),('JDG','00','20','site20',3,'2023-06-22'),('JDC','110110','18','site18',3,'2023-06-22');
insert into paimon_waybill_c values('JDA','01001','30','site30',4,'2023-06-21'),('JDD','10110','30','site30',4,'2023-06-21'),('JDG','0011','18','site18',4,'2023-06-22'),('JDH','1111','30','site30',4,'2023-06-23'),('JDI','0001','30','site30',4,'2023-06-24');
delete from paimon_waybill_c where dt='2023-06-21';

create table paimon_waybill_c_insert(
                                 waybillCode string,
                                 waybillSign string,
                                 siteCode string,
                                 siteName string,
                                 `timeStamp` bigint,
                                 dt string
)
    partitioned by(dt)
with(
    'bucket'='3',
    'bucket-key'='waybillCode',
    -- 经过几个cp后，进行一次full compaction
    'full-compaction.delta-commits'='10',
    'manifest.format'='orc'
);
insert into paimon_waybill_c_insert select * from hello_kafka;

insert into paimon_waybill_c_insert values('JDA','0010','11','site11',1,'2023-06-21'),('JDB','1111','15','site15',1,'2023-06-21'),('JDC','1110','11','site11',1,'2023-06-22');
insert into paimon_waybill_c_insert values('JDB','1101','20','site20',2,'2023-06-21'),('JDC','0101','20','site20',2,'2023-06-22');
insert into paimon_waybill_c_insert values('JDA','111100','20','site20',3,'2023-06-21'),('JDD','101010','20','site20',3,'2023-06-21'),('JDE','000000','20','site20',3,'2023-06-21'),('JDF','11','20','site20',3,'2023-06-22'),('JDG','00','20','site20',3,'2023-06-22'),('JDC','110110','18','site18',3,'2023-06-22');
insert into paimon_waybill_c_insert values('JDA','01001','30','site30',4,'2023-06-21'),('JDD','10110','30','site30',4,'2023-06-21'),('JDG','0011','18','site18',4,'2023-06-22'),('JDH','1111','30','site30',4,'2023-06-23'),('JDI','0001','30','site30',4,'2023-06-24');

SELECT * FROM paimon_waybill_c_insert /*+ OPTIONS('scan.mode'='from-timestamp','scan.timestamp-millis'='0') */;