# 总结：
# 向分区表overwrite数据
# 1.如果没有启动dynamic overwrite，则会先清空全表数据，再写入新数据
# 2.如果启动了dynamic overwrite，则只清空要写入的数据涉及到的分区，其余分区不会被清空。之后再写入新数据。
# 向非分区表overwrite数据
# 不论启不启动dynamic overwrite，都会先清空全表数据，再写入新数据

# insert overwrite的默认执行方式：先清空表中所有数据，再写入新的数据
create table mydb.hello_world(
                                 id bigint auto_increment,
                                 create_tm datetime,
                                 name varchar(100),
                                 age int,
                                 sex int
)
    primary key (id,create_tm)
partition by date_trunc('day',create_tm)
distributed by hash(id) buckets 2
order by (id);

create table mydb.hello_world_bak(
                                     id bigint auto_increment,
                                     create_tm datetime,
                                     name varchar(100),
                                     age int,
                                     sex int
)
    primary key (id,create_tm)
partition by date_trunc('day',create_tm)
distributed by hash(id) buckets 2
order by (id);

# 往多个分区写入数据
insert into mydb.hello_world values
     (default,now(),'hello',31,2),
     (default,now(),'world',22,1),
     (default,now()-interval 1 day,'wang wu',19,1),
     (default,now()-interval 2 day,'zhao liu',13,1);

# 可以查询到全部数据
# +------+-------------------+--------+---+---+
# |id    |create_tm          |name    |age|sex|
# +------+-------------------+--------+---+---+
# |100025|2025-04-23 17:29:42|zhao liu|13 |1  |
# |100022|2025-04-25 17:29:42|hello   |31 |2  |
# |100023|2025-04-25 17:29:42|world   |22 |1  |
# |100024|2025-04-24 17:29:42|wang wu |19 |1  |
# +------+-------------------+--------+---+---+
select * from mydb.hello_world;

# 往临时表里写一些数据
insert into mydb.hello_world_bak values
                                     (default,now(),'hello',31,2),
                                     (default,now()-interval 1 day,'world',22,1);

# 用overwrite的方式将临时表数据写入到主表
insert overwrite mydb.hello_world select * from mydb.hello_world_bak;

# 发现除了有新写入的数据，其余老数据都被清空了
# +--+-------------------+-----+---+---+
# |id|create_tm          |name |age|sex|
# +--+-------------------+-----+---+---+
# |7 |2025-04-25 17:32:54|hello|31 |2  |
# |8 |2025-04-24 17:32:54|world|22 |1  |
# +--+-------------------+-----+---+---+
select * from mydb.hello_world;

# 启动dynamic overwrite
# 启动dynamic overwrite后，执行方式变为只清空新数据中对应的分区的数据，再写入新数据。其他不在要写入的数据的分区中的数据不会被删除，有点像hive的执行方式。
# 清空后重新写入数据
truncate table mydb.hello_world;
truncate table mydb.hello_world_bak;
insert into mydb.hello_world values
                                 (default,now(),'hello',31,2),
                                 (default,now(),'world',22,1),
                                 (default,now()-interval 1 day,'wang wu',19,1),
                                 (default,now()-interval 2 day,'zhao liu',13,1);
insert into mydb.hello_world_bak values
                                     (default,now(),'hello',31,2),
                                     (default,now()-interval 1 day,'world',22,1);
# 关键的步骤：启动dynamic overwrite
SET dynamic_overwrite = true;

# 继续使用insert overwrite，观察写入后的数据
insert overwrite mydb.hello_world select * from mydb.hello_world_bak;

# 再次查询数据，发现bak表数据中的分区now()和now()-interval 1 day，在老表中都清空了，然后写入新的数据。但是bak中没有的老表的分区now()-interval 2 day则没有被清空，依然保留在数据中。
# +------+-------------------+--------+---+---+
# |id    |create_tm          |name    |age|sex|
# +------+-------------------+--------+---+---+
# |200002|2025-04-24 17:40:16|world   |22 |1  |
# |100029|2025-04-23 17:40:08|zhao liu|13 |1  |
# |200001|2025-04-25 17:40:16|hello   |31 |2  |
# +------+-------------------+--------+---+---+
select * from mydb.hello_world;

# 关于向非分区表overwrite数据的情况。总结：此时不管开不开启dynamic overwrite，都会先将全表数据完全清空，然后再写入新数据。
# 创建一个非分区表
create table mydb.hello_test(
    id bigint,
    name varchar(20),
    ts datetime
)
primary key (id)
distributed by hash(id) buckets 2;

# 向表中写入一批数据
INSERT INTO mydb.hello_test(id, name, ts) VALUES
                                              (1, 'Alice', '2023-01-15 08:30:45'),
                                              (2, 'Bob', '2023-02-20 14:25:10'),
                                              (3, 'Charlie', '2023-03-05 09:15:33'),
                                              (4, 'David', '2023-04-10 16:40:22'),
                                              (5, 'Eva', '2023-05-25 11:05:18'),
                                              (6, 'Frank', '2023-06-30 13:55:07'),
                                              (7, 'Grace', '2023-07-12 10:20:59'),
                                              (8, 'Henry', '2023-08-18 15:35:41'),
                                              (9, 'Ivy', '2023-09-22 07:45:30'),
                                              (10, 'Jack', '2023-10-31 23:10:15');
# 查询表中数据
# +--+-------+-------------------+
# |id|name   |ts                 |
# +--+-------+-------------------+
# |2 |Bob    |2023-02-20 14:25:10|
# |3 |Charlie|2023-03-05 09:15:33|
# |6 |Frank  |2023-06-30 13:55:07|
# |7 |Grace  |2023-07-12 10:20:59|
# |8 |Henry  |2023-08-18 15:35:41|
# |9 |Ivy    |2023-09-22 07:45:30|
# |1 |Alice  |2023-01-15 08:30:45|
# |4 |David  |2023-04-10 16:40:22|
# |5 |Eva    |2023-05-25 11:05:18|
# |10|Jack   |2023-10-31 23:10:15|
# +--+-------+-------------------+
select * from mydb.hello_test;

# 使用overwrite的方式向非分区表写入数据
INSERT overwrite mydb.hello_test(id, name, ts) VALUES
          (14, 'Katherine', '2024-01-01 00:00:01'),  -- 跨年时间点
          (15, '设备001', '2023-05-17 18:22:47'),    -- 中文+数字混合
          (16, 'O''Connor', '2023-11-11 11:11:11'),  -- 包含单引号(注意转义)
          (17, '', '2023-08-08 08:08:08'),           -- 空字符串测试
          (18, 'X', NULL);

# 再次查询，发现之前的数据完全被删除了，只存储了新的数据
# +--+---------+-------------------+
# |id|name     |ts                 |
# +--+---------+-------------------+
# |16|O'Connor |2023-11-11 11:11:11|
# |17|         |2023-08-08 08:08:08|
# |14|Katherine|2024-01-01 00:00:01|
# |15|设备001   |2023-05-17 18:22:47|
# |18|X        |null               |
# +--+---------+-------------------+
select * from mydb.hello_test;