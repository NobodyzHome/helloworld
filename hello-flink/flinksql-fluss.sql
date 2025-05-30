CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
       use catalog fluss_catalog;

create table mydb.hello_world(
                                        id bigint,
                                        name string,
                                        age int
)with(
    'bucket.num' = '2',
    'bucket.key' = 'id'
);

select * from hello_world /*+ OPTIONS('scan.startup.mode' = 'latest') */;

INSERT INTO mydb.hello_world (id, name, age) VALUES
                                                        (1, '张三', 28),
                                                        (2, '李四', 32),
                                                        (3, '王五', 24),
                                                        (4, '赵六', 45),
                                                        (5, '陈七', 19),
                                                        (6, '刘八', 37),
                                                        (7, '林九', 29),
                                                        (8, '吴十', 52),
                                                        (9, '周十一', 22),
                                                        (10, '徐十二', 41),
                                                        (11, '孙十三', 33),
                                                        (12, '杨十四', 26),
                                                        (13, '朱十五', 30),
                                                        (14, '黄十六', 47),
                                                        (15, '郑十七', 25),
                                                        (16, '谢十八', 39),
                                                        (17, '马十九', 31),
                                                        (18, '高二十', 28),
                                                        (19, '何二十一', 36),
                                                        (20, '邓二十二', 43);




drop table mydb.hello_world_pk;

CREATE temporary TABLE source_data_hello_world(
                                        id bigint,
                                        name string,
                                        age int
)
with (
    'connector'='datagen',
    'rows-per-second'='60',
    'fields.id.kind'='random',
    'fields.id.min'='100',
    'fields.id.max'='10000000',
    'fields.name.length'='5',
    'fields.age.kind'='random',
    'fields.age.min'='15',
    'fields.age.max'='40'
);

drop temporary table source_data_hello_world;

insert into mydb.hello_world select * from source_data_hello_world;

select age,count(*) cnt from mydb.hello_world group by age;


create table mydb.hello_world_pk(
    id int,
    name string,
    age int,
    sex int,
    dt string,
    primary key (id,dt) not enforced
)
PARTITIONED BY (dt)
with (
    'bucket.num' = '2',
    'bucket.key' = 'id',
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'day',
    'table.auto-partition.num-precreate' = '3',
    'table.auto-partition.num-retention' = '5',
    'table.auto-partition.time-zone' = 'Asia/Shanghai'
);

INSERT INTO mydb.hello_world_pk VALUES
                                    (1, '张三', 25, 1, '20250430'),
                                    (2, '李四', 30, 1, '20250430'),
                                    (3, '王五', 28, 1, '20250430'),
                                    (4, '赵六', 22, 0, '20250430'),
                                    (5, '钱七', 35, 0, '20250430'),
                                    (6, '孙八', 27, 1, '20250430'),
                                    (7, '周九', 31, 0, '20250430'),
                                    (8, '吴十', 29, 1, '20250501'),
                                    (9, '郑十一', 24, 0, '20250501'),
                                    (10, '王十二', 33, 1, '20250502');


show partitions mydb.hello_world_pk;

insert into mydb.hello_world_pk(id,name,age,dt) VALUES
                                    (1, 'hello', 23,  '20250430'),
                                    (9, 'world', 22,  '20250501');

select * from mydb.hello_world_pk where id =9 and dt='20250501';
select * from mydb.hello_world_pk where id =100 and dt='20250501';
select * from mydb.hello_world_pk where id =200 and dt='20250430';

CREATE temporary TABLE source_data_hello_world_pk(
    id int,
    name string,
    age int,
    sex int
)
with (
    'connector'='datagen',
    'rows-per-second'='120',
    'fields.id.kind'='random',
    'fields.id.min'='100',
    'fields.id.max'='1000',
    'fields.name.length'='5',
    'fields.age.kind'='random',
    'fields.age.min'='15',
    'fields.age.max'='40',
    'fields.sex.kind'='random',
    'fields.sex.min'='0',
    'fields.sex.max'='2'
);

drop temporary table source_data_hello_world_pk;

insert into mydb.hello_world_pk select *,'20250430' from source_data_hello_world_pk;

set sql-client.execution.result-mode=TABLEAU;


insert into mydb.hello_world_pk(id,name,age,dt) VALUES
                                                    (100, 'hello world', 30,  '20250501');

insert into mydb.hello_world_pk(id,name,age,sex,dt) VALUES
                                                    (100, 'hello fluss', 18,0,  '20250501');