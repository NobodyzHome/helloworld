-- 创建paimon catalog
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    -- 元数据存储方式，可选值为filesystem和hive，默认是filesystem，也就是将元数据存储到文件系统中。如果为hive的话，则是将元数据发送至hive的metastore服务。
    'metastore' = 'filesystem',
    -- 如果metastore为filesystem，元数据存储的目录
    'warehouse' = '/my-paimon',
    -- 默认使用的数据库。注意：paimon会自动在warehouse指定的目录下创建数据库的目录，创建的目录为【数据库名.db】。因此，这里指定的database不用带着.db后缀，否则创建的数据库目录就变为了app.db.db(假设database=app.db)。这点和hudi不同，hudi创建的数据库目录就是数据库名本身。
    'default-database'='app'
);
-- 切换使用的catalog为paimon catlog
use catalog paimon_catalog;
-- 使用paimon catalog创建table，paimon catalog除了会存储该表的元数据信息，还会在default-database目录下创建table目录。因此，paimon catalog不仅管理paimon表的元数据，还管理paimon表的物理存储目录。
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

-- 创建一个主键表，主键表有明显的特征：
-- 1.表中有主键
-- 2.配置中有merge-engine
-- 3.针对需要流读的情况，配置中有changelog-producer
-- 4.针对无法根据数据到来的顺序决定数据的新旧的情况（即会数据源头产生乱序），配置sequence.field来使用数据中的指定字段，作为判断两条数据哪个更新一些的依据
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
    'bucket'='2',
    -- LSM-tree中超过几个文件后，进行一次compaction
    --'num-sorted-run.compaction-trigger'='3',
    -- 经过几个cp后，进行一次full compaction
    'full-compaction.delta-commits'='3',
    -- 会产生changelog，例如先插入一条+I的数据，再插入相同key的+I数据的话，实际会产生两条数据，一条是-U，代表更新前的数据，另一条是+U，代表更新后的数据（更新的字段和未更新的字段都会有值）
    'changelog-producer'='full-compaction',
    'manifest.format'='orc'
);

-- 如果在使用paimon catalog，那么创建的表必须是paimon的connector。但是如果我想在paimon catalog下建一个其他connector的table要怎么做呢？可以使用TEMPORARY关键字，这样可以创建其他connector的表，同时paimon catalog也不会记录该表。
-- 下次重启sql-client客户端时，paimon catalog下就看不到这个表了。
create TEMPORARY table hello_kafka(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    `record_timestamp` TIMESTAMP_LTZ metadata from 'timestamp' virtual,
    `record_partition` BIGINT METADATA from 'partition' VIRTUAL,
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

set execution.checkpointing.interval='30 s';
insert into paimon_waybill_c select * from hello_kafka;

set execution.runtime-mode=batch;
set execution.runtime-mode=streaming;
/*
    读取总结：
    1.如果是流读，那么基本是找到需要读取的snapshot，找到后有两类处理
        a) 如果是带-full后缀的，则根据该snapshot生成快照数据文件（此时下发的数据都是+I类型的），然后依次读取该snapshot及之后的snapshot，并持续监听新的snapshot，读取其changelog文件
        b) 如果是不带-full后缀的，则依次读取该snapshot及之后的snapshot，并持续监听新的snapshot，读取其changelog文件
      可以理解为，如果你需要任务执行时先获取到全表信息，之后再获取该表的变化，可以使用带-full后缀的；而如果你只想获取从某个快照开始，表的变化情况，不需要全表信息，则可以使用不带-full后缀的。
    2.如果是批读，基本是找到需要读取的snapshot，然后根据该snapshot来生成快照数据内容

    也就是说：批读和带-full后缀的流读，都是需要根据snapshot生成快照数据内容的，这个过程比较耗费时间，因为需要读取所有的base file和delta file，进行数据合并，即MOR。
    而不带full的流读则处理比较轻，只需要读取snapshot中的changelog文件中的内容，没有MOR的动作。
*/
-- 'scan.mode'='latest'
-- 在流模式中，找到最新的snapshot(latest snapshot)，然后持续监听后续产生的snapshot（也就latest snapshot + 1），读取其中的changelog文件
-- 在批模式中，找到最新的snapshot(latest snapshot)，然后根据该snapshot中记录的base和delta manifest-file，生成当时的快照数据内容
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='latest') */;

-- 'scan.mode'='latest-full'
-- 在流模式中，找到最新的snapshot(latest snapshot)，根据最新的snapshot来生成快照数据内容，然后不断监听新产生的snapshot，读取其中的changelog文件
-- 在批模式中，处理方式和latest相同
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='latest-full') */;

-- 'scan.mode'='compacted-full'
-- 在流模式中，读取最后一个COMPACT类型的snapshot，根据其中的base和delta file生成快照数据内容，发送至下游（此时下发的数据都是+I类型的，反映当时的快照数据），并持续监听新的snapshot中的changelog。
-- 在批模式中，读取最后一个COMPACT类型的snapshot，根据其中的base和delta file生成快照数据内容，发送至下游。
-- 注意：使用该模式，对写入任务有要求，必须设置'changelog-producer'='full-compaction'以及'full-compaction.delta-commits'。因为只有这样，才会生成COMPACT类型的snapshot。
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='compacted-full') */;

-- 'scan.mode'='from-timestamp'
-- 在流模式下，找到第一个【小于等于】scan.timestamp-millis指定的时间戳的snapshot（使用snapshot中的timeMillis字段来比较），然后读取该snapshot后面的snapshot（不包含该snapshot，即snapshot+1），读取其中的change log文件（如果没有则忽略该snapshot），然后一直监听新产生的snapshot，读取其中的change log
-- 在批模式下，找到第一个【小于等于】scan.timestamp-millis指定的时间戳的snapshot，读取该snapshot（这里和流模式不一样，不是snapshot+1，而是获取到的snapshot），根据snapshot中记录的base和delta manifest-file，来生成该snapshot时的快照数据内容
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='from-timestamp','scan.timestamp-millis'='1694667960629') */;

-- 'scan.mode'='from-snapshot-full'
-- 在流模式下，from-snapshot-full是根据scan.snapshot-id指定的snapshot来生成快照当时的数据内容，然后依次读取后续snapshot的changlog文件，此后会持续监听新产生的snapshot，读取snapshot中的changelog文件并发送至下游。
-- 注意，在流模式下最开始生成的是快照数据，即已经将base file和change file合并合并后的数据，因此这些数据发送到下游，都是+I类型的。
-- 在批模式下，功能和'scan.mode'='from-snapshot'相同，都是根据指定snapshot生成当时的快照数据内容
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='5') */;

-- 'scan.mode'='from-snapshot'
-- 在流模式下，from-snapshot是直接读取指定snapshot中changelog文件，并持续监听后续产生的snapshot。如果指定的snapshot中没有changelog，则会直接继续依次读取后面的snapshot，如果该snapshot中有changelog文件，则读取并发送至下游。
-- 在批模式下，from-snapshot是直接根据指定的snapshot来生成当时的快照（就是根据对snapshot指定的base file和delta file中的数据进行合并，形成该快照时的表中的数据）
SELECT * FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='5') */;

-- 'scan.mode'='incremental'
-- 只在批模式下使用，查询在start snapshot之后（不包含start snapshot）到stop snapshot（包含stop snapshot）之间的改动内容。
-- 其实现就是遍历每一个需要的snapshot，从deltaManifestList或changelogManifestList中找到对应的data file或changelog file（取决于incremental-between-scan-mode的配置），将其读取出来。reader.withSnapshot(s).withMode(ScanMode.DELTA).read().splits();
-- incremental-between-scan-mode就是用来控制读取的内容，如果值为delta，则读取deltaManifestList对应的data file，如果值为changelog，则读取changelogManifestList对应的changelog file。reader.withSnapshot(s).withMode(ScanMode.CHANGELOG).read().splits();
select * from paimon_mysql_cdc/*+ OPTIONS('scan.mode'='incremental','incremental-between' = '58,59','incremental-between-scan-mode'='delta') */;
select * from paimon_mysql_cdc/*+ OPTIONS('scan.mode'='incremental','incremental-between-timestamp' = '1704263750108,1704274182534','incremental-between-scan-mode'='delta') */;


set execution.runtime-mode=batch;
SELECT * FROM paimon_waybill_c$snapshots;
SELECT * FROM paimon_waybill_c$files;


set sql-client.execution.result-mode=TABLEAU;
SELECT siteCode,count(*) cnt FROM paimon_waybill_c /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='1') */
group by siteCode;

set execution.runtime-mode=streaming;
insert into paimon_waybill_c values('JDA','0010','11','site11',1,'2023-06-21'),('JDB','1111','15','site15',1,'2023-06-21'),('JDC','1110','11','site11',1,'2023-06-22');
insert into paimon_waybill_c values('JDB','1101','20','site20',2,'2023-06-21'),('JDC','0101','20','site20',2,'2023-06-22');
insert into paimon_waybill_c values('JDA','111100','20','site20',3,'2023-06-21'),('JDD','101010','20','site20',3,'2023-06-21'),('JDE','000000','20','site20',3,'2023-06-21'),('JDF','11','20','site20',3,'2023-06-22'),('JDG','00','20','site20',3,'2023-06-22'),('JDC','110110','18','site18',3,'2023-06-22');
insert into paimon_waybill_c values('JDA','01001','30','site30',4,'2023-06-21'),('JDD','10110','30','site30',4,'2023-06-21'),('JDG','0011','18','site18',4,'2023-06-22'),('JDH','1111','30','site30',4,'2023-06-23'),('JDI','0001','30','site30',4,'2023-06-24');
delete from paimon_waybill_c where dt='2023-06-21';

-- 创建一个append only表，其最主要特征是没有主键
create table paimon_waybill_c_insert(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    record_timestamp TIMESTAMP_LTZ,
    record_partition bigint,
    dt string
)
    partitioned by(dt)
with(
    'bucket'='3',
    -- 指定用哪个字段作为分桶的依据，会使用该字段的值的hashcode%bucket数量来生成当前数据应该去哪个bucket中。如果没有配置
    'bucket-key'='waybillCode',
    -- 当文件数量或大小满足下面的配置时，则会产生一次compaction（不是full compaction），会产生一个COMPACT类型的snapshot。也就是说，不是只会由checkpoint产生COMPACT的snapshot(full compaction)，根据bucket的文件数量和大小，也会产生COMPACT的snapshot(非full compaction)
    -- 在append only表中使用，用于控制一个bucket中至少有几个data file，当这些data file的文件大小超过阈值(target-file-size)时，才会触发compaction。
    -- 这个配置的目的是减少compaction的触发，因为假如一个bucket中每个新进入的data file的大小都超过了阈值，那么每新进入一个data file，就会触发一次compaction。有了该配置，进入bucket的data file的size即使超过了阈值，也不会马上进行compaction，而是等compaction.min.file-num凑齐
    'compaction.min.file-num'='2',
    -- 在append only表中使用，用于控制一个bucket中最多有几个data file，即使这些文件的大小加起来都没有超过阈值(target-file-size)，也会触发compaction。
    -- 这个配置的目的是减少小文件数量，因为假设bucket中新进入的data file的size都很小，那么bucket中的data file会越来越多，而且这些文件加起来的大小也凑不到阈值。有了该配置，当bucket中的data file的大小即使一直很小，但是当数量超过了compaction.max.file-num，即使这些文件的大小加起来没有超过阈值，也会触发compaction。
    -- 注意：这两个配置是针对bucket的，即bucket-1中的文件满足该阈值后，就会触发COMPACT，此时只是对bucket-1进行合并，假如bucket-2没有满足阈值，就不会对bucket-2下的文件进行合并
    'compaction.max.file-num'='5',
    -- data file大小的阈值，当写入data file的数据量超过该阈值后，就会停止写入该文件，然后创建一个新的data file来写入。可以看到，该配置越大，文件会数量会越小，但每次compaction的数据量可能会很大，使compaction的压力增大。因此需要使用该配置，在文件数量和文件大小之间做出权衡的配置。
    -- 通过该配置可以看到，在一个bucket中，并不是一次checkpoint只产生一个data file。在一次checkpoint内，只要正在写入的文件超过该配置，就会生成一个新的data file。
    -- 注意：该配置不仅在append only表生效，在primary key表也生效
    'target-file-size'='100 kb',
    -- 该配置用于配置compaction的间隔，即每隔几次cp触发一次compaction。例如checkpoint1、2、3，在checkpoint3时触发compaction。此时如果在checkpoint3写入了数据，那么会产生两个snapshot，第一个是代表本次写入的snapshot（类型是APPEND），第二个是代表compaction的snapshot（类型是COMPACT）
    'full-compaction.delta-commits'='100'
);
insert into paimon_waybill_c_insert select * from hello_kafka;

insert into paimon_waybill_c_insert values('JDA','0010','11','site11',1,'2023-06-21'),('JDB','1111','15','site15',1,'2023-06-21'),('JDC','1110','11','site11',1,'2023-06-22');
insert into paimon_waybill_c_insert values('JDB','1101','20','site20',2,'2023-06-21'),('JDC','0101','20','site20',2,'2023-06-22');
insert into paimon_waybill_c_insert values('JDA','111100','20','site20',3,'2023-06-21'),('JDD','101010','20','site20',3,'2023-06-21'),('JDE','000000','20','site20',3,'2023-06-21'),('JDF','11','20','site20',3,'2023-06-22'),('JDG','00','20','site20',3,'2023-06-22'),('JDC','110110','18','site18',3,'2023-06-22');
insert into paimon_waybill_c_insert values('JDA','01001','30','site30',4,'2023-06-21'),('JDD','10110','30','site30',4,'2023-06-21'),('JDG','0011','18','site18',4,'2023-06-22'),('JDH','1111','30','site30',4,'2023-06-23'),('JDI','0001','30','site30',4,'2023-06-24');

SELECT * FROM paimon_waybill_c_insert/*+ options('scan.push-down'='false','scan.infer-parallelism'='false') */ where dt='2023-09-07' and waybillCode = 'JD0000000008';
SELECT * FROM paimon_waybill_c_insert where dt='2023-09-07' and waybillCode like 'JD0000000008';
SELECT * FROM paimon_waybill_c_insert where dt='2023-09-07' and record_partition >= 1 and waybillCode = 'JD0000000009';
SELECT * FROM paimon_waybill_c_insert where dt='2023-09-07' and record_partition = 0 ;
SELECT * FROM paimon_waybill_c_insert where dt='2023-09-07' and siteCode='10';

select * from paimon_waybill_c_insert;
select * from paimon_waybill_c_insert where dt='2023-09-12' and waybillCode='JD0000000009';
select * from paimon_waybill_c_insert /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='11') */ where dt='2023-09-12';
select * from paimon_waybill_c_insert /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='11') */ where dt='2023-09-12';

set table.optimizer.agg-phase-strategy=ONE_PHASE;
set table.exec.mini-batch.enabled=false;

create table paimon_waybill_c_insert_unaware(
                                        waybillCode string,
                                        waybillSign string,
                                        siteCode string,
                                        siteName string,
                                        record_timestamp TIMESTAMP_LTZ,
                                        record_partition bigint,
                                        dt string
)
    partitioned by(dt)
with(
    'bucket'='-1',
    'compaction.min.file-num'='2',
    'compaction.max.file-num'='5',
    'target-file-size'='100 kb',
    'full-compaction.delta-commits'='100'
);
insert into paimon_waybill_c_insert_unaware select * from hello_kafka;

create table paimon_waybill_c_sequence_field(
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
    'bucket'='2',
    'merge-engine'='deduplicate',
    -- 在进行LSM-TREE合并时，针对key相同的数据，默认情况下是根据数据的到来顺序来决定。但在分布式场景中，数据的真正顺序可能和数据的到来顺序不同（例如收集数据时，发送先收集到的数据延迟了，导致先收集的数据在后收集的数据的后面，导致数据乱序）
    -- 该配置可以指定数据中哪个字段用来指定数据的顺序，在LSM-TREE合并时，会使用该字段判断数据的新旧。
    'sequence.field' = 'timeStamp',
    'changelog-producer'='full-compaction',
    'full-compaction.delta-commits'='2',
    'manifest.format'='orc'
);

insert into paimon_waybill_c_sequence_field values('JDA','0010','11','site11',1,'2023-06-21'),('JDB','1111','15','site15',1,'2023-06-21'),('JDC','1110','11','site11',1,'2023-06-22');
insert into paimon_waybill_c_sequence_field values('JDB','1101','20','site20',0,'2023-06-21'),('JDC','0101','20','site20',2,'2023-06-22');
insert into paimon_waybill_c_sequence_field values('JDB','1101','50','site50',3,'2023-06-21'),('JDC','0101','60','site60',2,'2023-06-22'),('JDD','10110','30','site30',4,'2023-06-21'),('JDG','0011','18','site18',4,'2023-06-22');
insert into paimon_waybill_c_sequence_field values('JDB','0000','80','site80',4,'2023-06-22'),('JDC','0101','70','site70',3,'2023-06-22'),('JDD','01000','55','site55',5,'2023-06-21'),('JDG','1111','19','site19',5,'2023-06-22');
insert into paimon_waybill_c_sequence_field values('JDB','0000','100','site100',6,'2023-06-22'),('JDB','01','60','site60',6,'2023-06-21'),('JDC','0101','80','site80',6,'2023-06-22'),('JDF','0000','21','site21',6,'2023-06-22');
insert into paimon_waybill_c_sequence_field values('JDA','1100','18','site18',7,'2023-06-21'),('JDF','0000','11','site11',3,'2023-06-22');


SELECT * FROM paimon_waybill_c_sequence_field /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='4') */;
SELECT * FROM paimon_waybill_c_sequence_field /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='8') */;
SELECT * FROM paimon_waybill_c_sequence_field /*+ OPTIONS('scan.mode'='compacted-full') */;


/*
    一个snapshot写入的data file，会随着后续COMPACT类型的snapshot而变为可删除状态，因为COMPACT后，合并后的data file中肯定包含被合并的data file中的数据。
    但是为了实现time-travel，并不是有COMPACT类型的snapshot，就要马上把之前的snapshot都删除。然而，保留越多的snapshot，整个table目录中保留的数据文件就越多。我们都知道，当文件都存储在hdfs中，文件数量越多，namenode的压力就越大。
    因此，我们需要在文件存储性能和time travel可回放的范围，这两者中做出一个权衡。

    一个snapshot的delta manifest-list中，会记录对应的manifest-file文件，该文件记录了本次snapshot写入的data file，其中包含了该data file的kind类型，为0代表新添加的，为1代表可删除的。
    对于一个APPEND类型的snapshot，对应的manifest-file中记录的data file的kind属性都是0，也就是新添加的。而对于一个COMPACT类型的snapshot，其对应的manifest-file中，不仅记录了合并后的data-file，这些data file的kind都是0，还记录了本次是哪些data file被合并的，这些data file的kind属性都是1。
    当一个APPEND类型的snapshot被判定为过期，会从该snapshot中找到对应的manifest-file，发现其中记录的data file的kind都是0，因此在清理该snapshot时，不会删除data file，只删除snapshot文件和对应的manifest-list文件
    当一个COMPACT类型的snapshot被判定为过期，会从该snapshot中找到对应的manifest-file，找到kind为1的data file，删除对应的file文件，change log文件以及snapshot文件和对应的manifest-list文件

    注意：清除过期snapshot是在流写任务中进行的，具体来说是在流写任务中的GlobalCommitter中判断并执行的。

    判断逻辑：
    1.找到start snapshot：用latestSnapshot-maxRetain+1，得到起始判断的snapshot
    2.找到end snapshot：用latestSnapshot-minRetain，得到end snapshot
    3.循环遍历，找到最早有效的snapshot：从startSnapshot开始，依次遍历，看遍历的snapshot的timeMillis和当前时间的差是否小于等于timeRetained（代表该snapshot在需要保存的时间范围内），如果是，则找到该snapshot（记为snapshotX），终止遍历。如果不是，则继续遍历下一个snapshot
    4.snapshot清理：当找到snapshotX后，清理从earliest（包含）到snapshotX（不包含，可以理解snapshotX为需要的snapshot里，最早有效的snapshot）的相关文件
    5.如果找不到snapshot，则清理earliest snapshot（包含）到latestSnapshot-minRetain+1（不包含）之间的snapshot

    要知道，在没有compaction前，即使snapshot被删除了，snapshot对应的data file也不会被删除。换句话说，paimon表不会因为snapshot清理而丢数据。那snapshot清理的意义在于，你需要能回放多久的快照，假如你只需要全表最新数据，不需要回放历史的快照数据，
    那么你完全可以把snapshot.num-retained.max、snapshot.num-retained.min、snapshot.time-retained设置的小一些，这样能非常有效地减少文件的数量，表中的最新数据也不会丢，只不过你不能回放太久的快照数据了。
    啥时候你需要回放历史快照数据？就是你需要知道在过去某个快照的那个时刻，全表中的数据都是什么样的。如果没有这种需要，完全可以把snapshot的存留设置少一点。
*/
create table paimon_waybill_c_expire_snapshot(
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
    'bucket'='2',
    'merge-engine'='deduplicate',
    'changelog-producer'='full-compaction',
    'sequence.field' = 'timeStamp',
    'manifest.format'='orc',
    'full-compaction.delta-commits'='5',
    -- 控制最大的snapshot的数量，当产生新的snapshot，使snapshot数量超过该配置时，就会从最早的snapshot开始删除。例如该配置为3，当产生snapshot-1、2、3时，都不会产生snapshot的过期，但当新产生snapshot-4时，超过了该阈值，就会触发清理，将snapshot-1清除
    'snapshot.num-retained.max'='3',
    -- 控制至少要保留几个snapshot
    'snapshot.num-retained.min'='1',
    -- 控制一个snapshot的最长留存时间
    'snapshot.time-retained'='5 min'
);

insert into paimon_waybill_c_expire_snapshot select * from hello_kafka;

create table paimon_waybill_c_sort_partition(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    record_timestamp TIMESTAMP_LTZ,
    dt string
)
partitioned by(
    dt
)
with(
    'bucket'='5',
    'bucket-key'='waybillCode',
    -- 如果设置为true，则在生成split前，会先对partition进行排序，然后排在前面的partition生成的split就靠前。这样，在分配split时，partition靠前的split会先被分配和读取，最终形成按partition的顺序来读取数据的效果
    'scan.plan-sort-partition'='true'
);

insert into paimon_waybill_c_sort_partition select waybillCode, waybillSign, siteCode, siteName, record_timestamp, dt from hello_kafka;
-- 此时partition靠前的数据先被读取，可以看到的效果是先读dt='2023-09-13'的split，再读dt='2023-09-14'的split
select * from paimon_waybill_c_sort_partition/*+ options('scan.infer-parallelism'='false') */ where waybillCode='JD0000000007';
-- 通过hint来禁用scan.plan-sort-partition，可以看到的效果是先读dt='2023-09-13'的split，再读dt='2023-09-07'的split，可以发现，不是按分区的顺序读取数据的
select * from paimon_waybill_c_sort_partition/*+ options('scan.plan-sort-partition'='false','scan.infer-parallelism'='false') */ where waybillCode='JD0000000007';


create table paimon_waybill_c_external_log(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    ts bigint,
    dt string,
    primary key (waybillCode,dt) not enforced
)
partitioned by (
    dt
)
with(
    'bucket'='3',
    'merge-engine'='partial-update',
    'changelog-producer'='full-compaction',
    'full-compaction.delta-commits'='5',
    'sequence-field'='ts',
    'log.system' = 'kafka',
    'kafka.bootstrap.servers' = 'kafka-1:9092',
    'kafka.topic' = 'waybill_c_external_log',
    'log.system.auto-register' = 'true'
);

insert into paimon_waybill_c_external_log values('JDA','0010','11','site11',1,'2023-06-21'),('JDB','1111','15','site15',1,'2023-06-21'),('JDC','1110','11','site11',1,'2023-06-22');
insert into paimon_waybill_c_external_log values('JDB','1101','20','site20',0,'2023-06-21'),('JDC','0101','20','site20',2,'2023-06-22');
insert into paimon_waybill_c_external_log values('JDB','1101','30','site20',0,'2023-06-21');

create table paimon_waybill_c_lookup_changelog(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    record_timestamp TIMESTAMP_LTZ,
    record_partition bigint,
    dt string,
    primary key (dt,waybillCode) not enforced
)
partitioned by(
    dt
)
with(
    'merge-engine'='partial-update',
    'changelog-producer'='lookup',
    'bucket'='5',
    'sequence-field'='record_timestamp',
    'full-compaction.delta-commits'='100',
    'snapshot.num-retained.max'='10',
    'snapshot.num-retained.min'='3',
    'snapshot.time-retained'='5 min',
    'manifest.format'='orc'
);

insert into paimon_waybill_c_lookup_changelog select * from hello_kafka;
select * from paimon_waybill_c_lookup_changelog /*+ options('scan.infer-parallelism'='false') */;

create table paimon_waybill_c_compacted_full_changelog(
                                                  waybillCode string,
                                                  waybillSign string,
                                                  siteCode string,
                                                  siteName string,
                                                  record_timestamp TIMESTAMP_LTZ,
                                                  record_partition bigint,
                                                  dt string,
                                                  primary key (dt,waybillCode) not enforced
)
    partitioned by(
    dt
)
with(
    'bucket'='2',
    'merge-engine'='partial-update',
    'changelog-producer'='full-compaction',
    'full-compaction.delta-commits'='3',
    'snapshot.num-retained.max'='15',
    'snapshot.num-retained.min'='8',
    'snapshot.time-retained'='5 min',
    'manifest.format'='orc'
);

insert into paimon_waybill_c_compacted_full_changelog select * from hello_kafka;
select * from paimon_waybill_c_compacted_full_changelog /*+ OPTIONS('scan.mode'='latest-full') */;
select * from paimon_waybill_c_compacted_full_changelog /*+ OPTIONS('scan.snapshot-id'='23') */;
SELECT * FROM paimon_waybill_c_compacted_full_changelog /*+ OPTIONS('scan.mode'='compacted-full') */;

create table paimon_waybill_c_expire_partition(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    record_timestamp TIMESTAMP_LTZ,
    record_partition bigint,
    dt string,
    primary key (dt,waybillCode) not enforced
)
partitioned by (dt)
with(
    'bucket'='3',
    'merge-engine'='deduplicate',
    'changelog-producer'='full-compaction',
    'full-compaction.delta-commits'='20',
    -- 分区过期时间，会用当前时间减去该值，看是否大于遍历的分区的时间，如果是的话，则删除该分区
    'partition.expiration-time' = '1 d',
    -- 分区过期的检查间隔，隔该配置的时间，才会再进行一次分区过期处理
    'partition.expiration-check-interval' = '1 min',
    -- 分区时间的格式，需要和partition.timestamp-pattern配合使用。假设dt字段的值是2023-10-18，那么formatter则应该是yyyy-MM-dd，用于将dt字段的值解析成时间
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    -- 将表中哪个字段作为分区时间解析的字段。假设表的分区字段是year,month,day这三个字段，那么该配置就可以配置为$year-$month-$day，然后partition.timestamp-formatter配置成yyyy-MM-dd
    'partition.timestamp-pattern' = '$dt',
    'snapshot.num-retained.max'='5',
    'snapshot.num-retained.min'='2',
    'snapshot.time-retained'='5 min',
    'manifest.format'='orc'
);

drop table paimon_waybill_c_expire_partition;

select * from paimon_waybill_c_expire_partition/*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='8') */;

-- 当一个分区过期后，不是直接把分区的文件全部删除。而是创建一个OVERWRITE类型的snapshot，在里面写入的manifest-file中，记录了该过期分区的所有data file，这些data file的kink属性都是1，代表已经被删除了。
-- 这样，读取该snapshot时，就不会读取delete类型的data file，实现了过期的分区无法被查询到的目的，但是过期的分区里，data file依然存在。
-- 再配合snapshot过期，就可以实现把过期的分区的data file都删除的目的（因为这些file的kink=1）
insert into paimon_waybill_c_expire_partition select * from hello_kafka;

select * from paimon_waybill_c_expire_partition/*+ OPTIONS('streaming-read-overwrite'='false') */;
select * from paimon_waybill_c_expire_partition/*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='5') */;

create table paimon_waybill_c_overwrite(
    id string,
    name string,
    dt string,
    ts bigint,
    primary key (dt,id) not enforced
)
    partitioned by (dt)
with(
    'bucket'='3',
    'merge-engine'='partial-update',
    'changelog-producer'='none',
    'manifest.format'='orc'
);

insert into paimon_waybill_c_overwrite values('1','hello','2023-10-25',1),('2','paimon','2023-10-26',1);
insert into paimon_waybill_c_overwrite values('3','zhangsan','2023-10-26',1),('3','lisi','2023-10-27',1);
insert into paimon_waybill_c_overwrite values('4','wangwu','2023-10-26',1),('5','zhaoliu','2023-10-27',1);

-- 注意：使用overwrite清理数据，并不是像hive那样直接把老数据删除。而是创建了一个新的OVERWRITE类型的snapshot，该snapshot对应的manifest file中，记录了哪些data file可以被删除（kind=1）。
-- 在读取该table时，kind=1的data file是不会被读取的，也就达到了被overwrite的老数据被“清除”的效果
-- 当该语句执行完成后，会生成两个snapshot，第一个是OVERWRITE类型的，对应的manifest file记录了哪些data file可以被删除（就是被覆盖的分区下的data file）。而第二个snapshot是APPEND类型的，记录了新写入的数据写到了哪个data file中。
-- 此种方式是动态分区覆盖，即根据要写入的value中的分区字段，决定要覆盖哪个分区。在这里是将dt='2023-10-25'、dt='2023-10-26'两个分区覆盖。
insert overwrite paimon_waybill_c_overwrite values('1','hello world','2023-10-25',2),('2','tt','2023-10-26',2);
-- 清除一个分区下的数据。dynamic-partition-overwrite参数用于控制在overwrite时，是否使用动态分区来overwrite，也就是根据写入的value中的分区字段的值，来决定要overwrite哪个分区。
-- 如果该参数为false，则根据partition( dt='2023-10-25')中给出的静态分区来决定要overwrite哪个分区，而不是再动态决定了
insert overwrite paimon_waybill_c_overwrite /*+ OPTIONS('dynamic-partition-overwrite'='false') */  partition( dt='2023-10-25') select id,name,ts from paimon_waybill_c_overwrite where false;
-- 清除全表的数据
INSERT OVERWRITE paimon_waybill_c_overwrite /*+ OPTIONS('dynamic-partition-overwrite'='false') */ SELECT * FROM paimon_waybill_c_overwrite WHERE false;
select * from paimon_waybill_c_overwrite;

create table paimon_mysql_cdc(
     id int,
     name string,
     age int,
     dt string,
     PRIMARY KEY(dt,id) NOT ENFORCED
) partitioned by(
    dt
)
with(
    'auto-create' = 'true',
    'merge-engine'='deduplicate',
    'bucket'='2',
    'changelog-producer'='input',
    'full-compaction.delta-commits'='10',
    'manifest.format'='orc'
);

drop table paimon_mysql_cdc;

CREATE TEMPORARY TABLE mysql_cdc_table (
                            id int,
                            name string,
                            age int,
                            dt string,
                            PRIMARY KEY(dt,id) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = 'my-mysql',
      'port' = '3306',
      'username' = 'root',
      'password' = '123456',
      'database-name' = 'test_database',
      'table-name' = 'test_table');

insert into paimon_mysql_cdc select * from mysql_cdc_table;

set execution.runtime-mode=batch;
select * from paimon_mysql_cdc/*+ OPTIONS('scan.mode'='incremental','incremental-between' = '23,24','incremental-between-scan-mode'='changelog') */;
insert overwrite paimon_mysql_cdc /*+ OPTIONS('dynamic-partition-overwrite'='false') */ partition(dt='2024-01-02') select id,name,age from paimon_mysql_cdc where dt='2024-01-03' and id<=100;
insert overwrite paimon_mysql_cdc /*+ OPTIONS('dynamic-partition-overwrite'='false') */ partition(dt='2024-01-02') select id,name,age from paimon_mysql_cdc where false;
select * from paimon_mysql_cdc where dt='2024-01-02' and id in (89,90);
select * from paimon_mysql_cdc where dt='2024-01-03' and id in (89,90);

set execution.runtime-mode=streaming;
select * from paimon_mysql_cdc/*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='17') */;
select * from paimon_mysql_cdc/*+ OPTIONS('scan.mode' = 'latest') */;
select * from paimon_mysql_cdc/*+ OPTIONS('scan.mode' = 'latest-full') */;
select age,count(id) cnt from paimon_mysql_cdc group by age;

CREATE TEMPORARY TABLE source_data_table(
    id int,
    name string,
    age int,
    ts timestamp
)
with (
    'connector'='datagen',
    'rows-per-second'='1',
    'fields.id.kind'='sequence',
    'fields.id.start'='10',
    'fields.id.end'='99999',
    'fields.name.length'='6',
    'fields.age.kind'='random',
    'fields.age.min'='15',
    'fields.age.max'='30'
);

CREATE TABLE test_table (
                            id int,
                            name string,
                            age int,
                            ts timestamp,
                            PRIMARY KEY(id) NOT ENFORCED
) WITH (
      'connector'='upsert-kafka',
      'properties.bootstrap.servers'='kafka-1:9092',
      'topic'='hello_world',
      'key.format'='csv',
      'value.format'='json'
      );

insert into test_table select * from source_data_table;

drop TEMPORARY table source_data_table;

create table paimon_table(
     id int,
     name string,
     age int,
     dt string,
     primary key (dt,id) not enforced
)
partitioned by(
    dt
)
with(
    'bucket'='2',
    'merge-engine'='deduplicate',
    'changelog-producer'='full-compaction',
    'full-compaction.delta-commits'='5',
    'manifest.format'='orc'
);

drop table paimon_table;

insert into paimon_table select *,cast(current_date as string) dt from source_data_table;

set execution.runtime-mode=batch;
delete from paimon_table where dt=cast(current_date as string) and id in (10,12);


CREATE TEMPORARY TABLE jdbc_table (
                            id int,
                            name string,
                            age int,
                            dt string,
                            PRIMARY KEY(dt,id) NOT ENFORCED
) WITH (
      'connector' = 'jdbc',
      'url'='jdbc:mysql://my-mysql:3306/test_database',
      'table-name' = 'test_table',
      'username' = 'root',
      'password' = '123456');

insert into jdbc_table select *,cast(current_date as string) dt from source_data_table;


select *,date_format(create_time,'yyyy-MM-dd') dt from student_info;

create table paimon_none_changelog(
    id int,
    name string,
    age int,
    dt string,
    primary key (dt,id) not enforced
)
partitioned by(
    dt
)
with(
    'bucket'='3',
    'merge-engine'='deduplicate',
    'changelog-producer'='none',
    'manifest.format'='orc'
);
insert into paimon_none_changelog select *,cast(current_date as string) dt from source_data_table;
select * from paimon_none_changelog /*+ options('consumer-id'='testid','scan.remove-normalize'='true') */;

-- 我们可以使用rowkind.field字段，来指出table中哪个字段用于生成这行数据的RowKind，默认是无，则使用数据原本的RowKind
-- rowkind.field字段对应的值只能是+I、-U、+U、-D
-- 实际应用：通过数据源中数据类型字段来生成changelog，而不是用paimon来生成changelog
-- 1.在写入paimon前，也就是select时，根据根据数据源的操作类型字段，生成一个转换成【+I、-U、+U、-D】这种值的字段（例如opt，逻辑为case RAND_INTEGER(4) when 0 then '+I' when 1 then '-U' when 2 then '+U' when 3 then '-D' else '+I' end opt）
-- 2.设置'rowkind.field'='opt',这样writer在向paimon表写入时，就能识别这行数据的RowKind并赋值
-- 3.设置'changelog-producer'='input'，这样将该数据写入data file和changelog file时，就知道该行数据的kind了
create table paimon_input_table(
                             id int,
                             name string,
                             age int,
                             dt string,
                             opt string,
                             primary key (dt,id) not enforced
)
    partitioned by(
    dt
)
with(
    'bucket'='1',
    'merge-engine'='deduplicate',
    -- 将changelog-producer设置为input，从数据源的数据中获取RowKind
    'changelog-producer'='input',
    'full-compaction.delta-commits'='2',
    -- 根据paimon表中指定字段来生成数据的RowKind
    'rowkind.field'='opt',
    'manifest.format'='orc'
);

SELECT * FROM paimon_input_table /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='1') */;
insert into paimon_input_table select *,cast(current_date as string) dt,case RAND_INTEGER(4) when 0 then '+I' when 1 then '-U' when 2 then '+U' when 3 then '-D' else '+I' end opt from source_data_table;
select name,age,count(*) cnt,grouping_id(name,age) from paimon_input_table group by grouping sets ((name),(age))

create table paimon_input_table_filter(
                                   id int,
                                   name string,
                                   age int,
                                   dt string,
                                   opt string,
                                   primary key (dt,id) not enforced
)
    partitioned by(
    dt
)
with(
    'bucket'='1',
    'merge-engine'='deduplicate',
    'changelog-producer'='input',
    'full-compaction.delta-commits'='2',
    'manifest.format'='orc'
);

insert into paimon_input_table_filter select * from paimon_input_table /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='2') */ where age<=22;

select * from paimon_input_table where id>=870;


create TEMPORARY table hello_world_kafka(
    id int,
   name string,
       age int
)with(
     'connector'='kafka',
     'properties.bootstrap.servers'='kafka-1:9092',
     'topic'='hello_world',
     'key.format'='raw',
     'key.fields'='waybillCode',
     'value.format'='json',
     'scan.startup.mode'='latest-offset'
);


create table source_sr(
    id int,
   name string,
       age int,
       ts timestamp
)with(
     'connector'='starrocks',
     'jdbc-url'='jdbc:mysql://my-starrocks:9030',
     'scan-url'='my-starrocks:8030',
     'username'='root',
     'password'='',
     'database-name'='mydb',
     'table-name'='hello_world'
);

CREATE CATALOG paimon_hdfs_catalog WITH (
    'type' = 'paimon',
    -- 元数据存储方式，可选值为filesystem和hive，默认是filesystem，也就是将元数据存储到文件系统中。如果为hive的话，则是将元数据发送至hive的metastore服务。
    'metastore' = 'filesystem',
    -- 如果metastore为filesystem，元数据存储的目录
    'warehouse' = 'hdfs://namenode:9000/my-paimon',
    -- 默认使用的数据库。注意：paimon会自动在warehouse指定的目录下创建数据库的目录，创建的目录为【数据库名.db】。因此，这里指定的database不用带着.db后缀，否则创建的数据库目录就变为了app.db.db(假设database=app.db)。这点和hudi不同，hudi创建的数据库目录就是数据库名本身。
    'default-database'='app'
);

use catalog paimon_hdfs_catalog;


create table paimon_append(
    id int,
    name string,
    dt date
)
    partitioned by(dt)
with(
    'bucket'='3',
    'bucket-key'='id',
    'compaction.min.file-num'='2',
    'compaction.max.file-num'='5',
    'target-file-size'='100 kb',
    'full-compaction.delta-commits'='100'
);

set execution.runtime-mode=batch;

insert into app.paimon_append values(1,'hello',current_date);

select * from app.paimon_append;

create table app.paimon_pk_tbl(
    dt string,
    id int,
    name string,
    age int,
    sex int,
    primary key (dt,id) not enforced
)
partitioned by(dt)
with(
    'bucket'='2',
    'bucket-key'='id',
    'merge-engine'='partial-update'
);

drop table app.paimon_pk_tbl;

insert into app.paimon_pk_tbl values('2024-10-30',1,'hello',11,1),('2024-10-30',2,'world',15,2);
insert into app.paimon_pk_tbl values('2024-10-30',1,'hello world',20,cast(null as int));
insert into app.paimon_pk_tbl values('2024-10-30',2,'hello paimon',cast(null as int),cast(null as int));