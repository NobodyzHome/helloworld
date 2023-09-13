-- 创建hudi的外部表，即使用default_catalog管理hudi表的元数据。这样的话，default_catalog是不会在hudi表创建或删除后，创建或删除hudi表对应的hdfs目录的。
-- 因此，可以这么理解，使用default_catalog管理hudi表，只是管理hudi表的元数据，不管理hudi表对应的物理存储目录。
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

create table hello_hudi(
    id string primary key not enforced ,
    age int,
    name string,
    dt string
)partitioned by(dt)
with(
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
    'changelog.enabled'='false',
    'cdc.enabled'='false',
    'metadata.enabled'='false'
);

insert into hello_hudi values('1',10,cast(null as string),'2023-10-11');
insert into hello_hudi values('1',cast(null as int),'hello','2023-10-11');
insert into hello_hudi values('1',cast(null as int),'hello hudi','2023-10-11');
insert into hello_hudi values('1',cast(null as int),'hudi hello','2023-10-11');
insert overwrite hello_hudi values('1',20,cast(null as string),'2023-10-11');
update hello_hudi /*+ options('read.streaming.enabled'='false') */ set name='hello cdc' where id='1';
select * from hello_hudi /*+ options('hoodie.datasource.query.type'='incremental') */;

create table hello_hudi_insert(
                                  id string primary key not enforced ,
                                  age int,
                                  name string,
                                  dt string
)partitioned by(dt)
with(
    'table.type' = 'MERGE_ON_READ',
    'write.operation'='insert',
    'write.bulk_insert.sort_input'='false',
    'clustering.schedule.enabled'='true',
    'clustering.async.enabled'='true',
    'clustering.delta_commits'='2',
    'clean.async.enabled'='true',
    'clean.policy'='KEEP_LATEST_COMMITS',
    'clean.retain_commits'='1',
    'changelog.enabled'='false',
    'cdc.enabled'='false',
    'metadata.enabled'='false'
);

insert into hello_hudi_insert values('1',10,cast(null as string),'2023-10-11');
insert into hello_hudi_insert values('1',cast(null as int),'hello','2023-10-11');
insert into hello_hudi_insert values('1',cast(null as int),'hello hudi','2023-10-11');
insert into hello_hudi_insert values('1',cast(null as int),'hudi hello','2023-10-11');

create table default_catalog.default_database.kafka_hello_hudi(
                                                                  id string primary key not enforced ,
                                                                  age int,
                                                                  name string,
                                                                  dt string
)with(
     'connector'='upsert-kafka',
     'properties.bootstrap.servers'='kafka-1:9092',
     'topic'='hello-hudi',
     'key.format'='raw',
     'value.format'='json'
     );

insert into hello_hudi_insert select * from default_catalog.default_database.kafka_hello_hudi;

{"id":"1","dt":"2023-10-11","age":10}
{"id":"1","dt":"2023-10-11","age":30}
{"id":"1","dt":"2023-10-11","name":"hello"}
{"id":"1","dt":"2023-10-11","name":"hello hudi","age":20}
{"id":"1","dt":"2023-10-11","name":"hello hudi world","age":35}

-- 注意：在使用hudi catalog时，不能创建除了hudi以外的connector的表，否则hudi catalog会把所有表的connector修改为hudi
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

-- catalog.path指定的目录必须已存在，否则创建catalog时会报错
CREATE CATALOG my_hudi WITH (
    'type'='hudi',
    -- 元数据的存储模式，可选项为dfs或hms。当使用dfs时，是将元数据记录在catalog.path指定的目录中。当使用hms时，是将元数据发送至hive的hms服务中。
    'mode'='dfs',
    -- 当mode=dfs时，指定catalog所在的文件系统的目录，该目录必须存在，否则创建时会报错
    'catalog.path'='/my-hudi/hudi_catalog',
    -- 默认使用的数据库
    'default-database'='app.db'
);
-- 将catalog切换至hudi的catalog，使用hudi catalog来创建database或table，不仅会存储hudi表的元数据，还会在database或table对应的hdfs上创建目录，delete database或table也是一样
use catalog my_hudi;
-- 使用hudi catalog创建database，除了会在元数据中记录database的元数据，hudi catalog还会去对应目录下创建database的目录
create database if not exists `app.db`;
-- 使用hudi catalog创建table，除了会在元数据中记录table的元数据，hudi catalog还会在/hudi catalog/current data base目录下创建hudi表
-- 因此，在hudi catalog下，不需要指定connector属性，因为所有的table都是hudi connector。也不需要指定path属性，hudi catalog会自动根据表名来创建表的目录。
create table if not exists hudi_waybill_c(
    waybillCode string,
    waybillSign string,
    siteCode string,
    siteName string,
    `timeStamp` bigint,
    dt string,
    primary key(waybillCode) not enforced
)
partitioned by(
    dt
)
with(
    'table.type'='MERGE_ON_READ',
    'index.type'='BUCKET',
    'hoodie.index.bucket.engine'='SIMPLE',
    'hoodie.bucket.index.num.buckets'='3',
    'write.operation'='upsert',
    'write.precombine'='true',
    'precombine.field'='timeStamp',
    'payload.class'='org.apache.hudi.common.model.PartialUpdateAvroPayload',
    'hoodie.payload.event.time.field'='timeStamp',
    'hoodie.payload.ordering.field'='timeStamp',
    'compaction.trigger.strategy'='num_commits',
    'compaction.delta_commits'='3',
    'clean.async.enabled'='true',
    'clean.policy'='KEEP_LATEST_FILE_VERSIONS',
    'clean.retain_file_versions'='2',
    'hoodie.metadata.enable'='false',
    'changelog.enabled'='false',
    'cdc.enabled'='false'
);

-- 在使用hudi catalog的情况下，如果想创建其他connector的table，可以使用TEMPORARY关键字。该table不会记录到hudi catalog中。
create TEMPORARY table kafka_waybill_c(
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
set sql-client.execution.result-mode=TABLEAU;
-- 流写
insert into hudi_waybill_c select * from kafka_waybill_c;
-- 流读
select * from hudi_waybill_c /*+ options('read.streaming.enabled'='true','read.streaming.check-interval'='10') */;

-- 增量读
select * from hudi_waybill_c /*+ options('hoodie.datasource.query.type'='incremental','read.streaming.enabled'='false','read.streaming.check-interval'='10','read.start-commit'='20230913083248567','read.end-commit'='20230913083248567') */;
select * from hudi_waybill_c /*+ options('hoodie.datasource.query.type'='incremental','read.streaming.enabled'='false','read.streaming.check-interval'='10','read.start-commit'='20230913083358417','read.end-commit'='20230913083358417') */;
-- 增量读可以读历史的几个instant，将这些instant对应的数据文件合并
select * from hudi_waybill_c /*+ options('hoodie.datasource.query.type'='incremental','read.streaming.enabled'='false','read.streaming.check-interval'='10','read.start-commit'='20230913083248567','read.end-commit'='20230913083358417') */;
-- 增量读设置read.start-commit后，不论是流读还是披读，都只从指定的instant开始读起，历史的instant不会读到。也就是说，增量读的话，读取到的数据可能不是全表里全量的数据
-- 使用增量读可以获取在历史instant中，在每一个instant中发生的数据变化
select * from hudi_waybill_c /*+ options('hoodie.datasource.query.type'='incremental','read.streaming.enabled'='true','read.streaming.check-interval'='10','read.start-commit'='20230913083358417') */;

-- 优化读。优化读是读全表的所有数据，只不过在读取每一个bucket时，只读取最新slice中的parquet文件，没有数据合并的过程。因此读取效率很高，但时效差。
select * from hudi_waybill_c /*+ options('hoodie.datasource.query.type'='read_optimized') */;
-- 快照读。快照读也是读取全表的所有数据，只不过在读取每一个bucket时，既读parquet文件，也读log文件，然后进行数据合并。因此读取效率低，但时效高。
select * from hudi_waybill_c /*+ options('hoodie.datasource.query.type'='snapshot') */;
