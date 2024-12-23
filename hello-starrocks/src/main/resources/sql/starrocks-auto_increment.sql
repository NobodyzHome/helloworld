# 使用自增键作为主键。此时主要使用自增键生成全局唯一的数据ID。
create table mydb.use_auto_increment(
    dt date,
    id bigint auto_increment,
    name varchar(200),
    age int,
    sex int,
    tm datetime
)
primary key (dt,id)
partition by date_trunc('day',dt)
distributed by hash(id) buckets 2
order by (age);

# 第一次load
# 在向包含自增字段的表写入数据时，可以通过在insert字段列表中不指定该字段，来让sr给该字段生成自增的值
insert into mydb.use_auto_increment(dt, name, age, sex, tm)
values ('2024-12-23', 'zhang san', 15, 1, now()),
       ('2024-12-23', 'li si', 20, 2, now()),
       ('2024-12-22', 'wang wu', 21, 1, now());

# +----------+--+---------+---+---+-------------------+
# |dt        |id|name     |age|sex|tm                 |
# +----------+--+---------+---+---+-------------------+
# |2024-12-22|6 |wang wu  |21 |1  |2024-12-23 16:47:39|
# |2024-12-23|4 |zhang san|15 |1  |2024-12-23 16:47:39|
# |2024-12-23|5 |li si    |20 |2  |2024-12-23 16:47:39|
# +----------+--+---------+---+---+-------------------+
select * from mydb.use_auto_increment;

# 第二次load
# 在向包含自增字段的表写入数据时，也可以不指定insert列表，然后在自增字段的赋值中使用关键字【default】，也可让sr给该字段生成自增的值
insert into mydb.use_auto_increment
values ('2024-12-23', default, 'aa', 21, 2, now()),
       ('2024-12-23', default, 'bb', 30, 2, now()),
       ('2024-12-23', default, 'cc', 11, 1, now()),
       ('2024-12-22', default, 'dd', 17, 1, now()),
       ('2024-12-22', default, 'ee', 23, 1, now());

# 说明下为什么第一批写入的id是4、5、6，第二批写入的id是100001、100002这种。
# 这就涉及到load的原理和auto_increment的原理
# 在发起load后，FE挑选一个BE作为coordinator BE，由其向客户端拉取要写入的数据，并将数据分发给不同的Executor BE进行数据写入。
# auto_increment的原理是每一个BE会缓存一个自增编码池，比如有3个BE，第一个BE的池子是[1,100000]，第二个BE的池子是[100001,200000]，第三个BE的池子是[200001,300000]。
# 在load时，coordinator BE会先从该BE自身的自增编码池中拿取自增编码，赋值给数据的自增字段，然后才会将数据分发给不同的Executor BE。由于这两次load对应的coordinator BE不一样，导致自增编码池不一样，最终给自增字段赋的值自然也不一样。
# +----------+------+---------+---+---+-------------------+
# |dt        |id    |name     |age|sex|tm                 |
# +----------+------+---------+---+---+-------------------+
# |2024-12-23|4     |zhang san|15 |1  |2024-12-23 16:47:39|
# |2024-12-23|5     |li si    |20 |2  |2024-12-23 16:47:39|
# |2024-12-22|6     |wang wu  |21 |1  |2024-12-23 16:47:39|
# |2024-12-23|100001|aa       |21 |2  |2024-12-23 16:50:06|
# |2024-12-23|100002|bb       |30 |2  |2024-12-23 16:50:06|
# |2024-12-23|100003|cc       |11 |1  |2024-12-23 16:50:06|
# |2024-12-22|100004|dd       |17 |1  |2024-12-23 16:50:06|
# |2024-12-22|100005|ee       |23 |1  |2024-12-23 16:50:06|
# +----------+------+---------+---+---+-------------------+
select * from mydb.use_auto_increment;

# 自增字段作为一个普通字段。此时该表主要被作为一个全局字典表，这个自增字段是字典中的value。
# 例如下表中是一个从字符串erp字段到bigint类型的id字段的全局字典表。数据源表使用该字典表，将数据中的erp字段转为bigint类型的value，然后就可以通过bitmap来实现精准去重。比直接count(distinct erp)性能要好很多。
create table mydb.erp_dict(
    # 字典的key字段
    erp varchar(500),
    # 字典的value字段
    erp_int_id bigint auto_increment
)
primary key (erp)
distributed by hash(erp) buckets 2;

# 第一次load
insert into mydb.erp_dict(erp)
values ('zhang san'),
       ('li si'),
       ('wang wu');

# +---------+----------+
# |erp      |erp_int_id|
# +---------+----------+
# |zhang san|1         |
# |li si    |2         |
# |wang wu  |3         |
# +---------+----------+
select * from mydb.erp_dict order by erp_int_id;

# 第二次load
insert into mydb.erp_dict
values ('zhao liu', default),
       ('aa', default),
       ('bb', default),
       ('cc', default),
       ('dd', default);

# +---------+----------+
# |erp      |erp_int_id|
# +---------+----------+
# |zhang san|1         |
# |li si    |2         |
# |wang wu  |3         |
# |zhao liu |200001    |
# |aa       |200002    |
# |bb       |200003    |
# |cc       |200004    |
# |dd       |200005    |
# +---------+----------+
select * from mydb.erp_dict order by erp_int_id;

# 关于主键覆盖导致自增列更新
# 第一次load
insert into mydb.erp_dict(erp) values('hello world');
# 为该数据分配了自增字段值
# +-----------+----------+
# |erp        |erp_int_id|
# +-----------+----------+
# |hello world|200006    |
# +-----------+----------+
select * from mydb.erp_dict where erp='hello world';
#  第二次load，由于主键相同，会覆盖此前的数据
insert into mydb.erp_dict(erp) values('hello world');
# 可以看到，数据被覆盖了，同时生成的自增字段值也变了
# 这说明每次load时，都会由coordinator BE来为自增字段赋值，不管主键字段是否相同
# +-----------+----------+
# |erp        |erp_int_id|
# +-----------+----------+
# |hello world|9         |
# +-----------+----------+
select * from mydb.erp_dict where erp='hello world';