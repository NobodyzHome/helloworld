-- 启用 RBO 阶段表裁剪。
SET enable_rbo_table_prune=true;
-- 启用 CBO 阶段表裁剪。
SET enable_cbo_table_prune=true;
-- 为主键表上的 UPDATE 语句启用 RBO 阶段表裁剪。
SET enable_table_prune_on_update = true;

# 裁剪表。通常为主键模型或具有唯一约束的明细模型
create table mydb.dept(
    id bigint,
    name varchar(500),
)
    primary key (id)
distributed by hash(id) buckets 1;

# 保留表
create table mydb.emp(
                         id bigint,
                         name varchar(200),
                         sex int,
                         # 当使用inner join进行表裁剪时，必须在建表时显式地将外键字段设置为not null
    dept_id bigint not null
)
    duplicate key (id)
distributed by random buckets 1
properties(
    # 为保留表的指定字段设置为外键
   "foreign_key_constraints" =  "(dept_id) REFERENCES dept(id)"
);

alter table mydb.dept
    add column province_id int;

# 保留表。明细模型，通过unique_constraints指定唯一键。
create table mydb.province(
                              id bigint,
                              name varchar(500)
)
    duplicate key(id)
distributed by random buckets 1
properties(
    # 针对明细模型，可以使用unique_constraints设置唯一约束。但该属性只是用于给优化器增加优化提示，sr实际不用校验该字段的唯一性，需要由用户导入数据时保证数据的唯一性。
    "unique_constraints" = "id"
);

select * from mydb.dept;

insert into mydb.dept values(1,'dept_1'),(2,'dept_2');
insert into mydb.emp values(1,'zhang san',1,1),(2,'li si',2,2),(3,'wang wu',1,1),(4,'zhao liu',2,1),(5,'duo duo',1,2);
insert into mydb.emp values(6,'diu diu',1,3);

# 我们把CTE里面的逻辑看成目前已有的代码，代码里有对emp表的处理逻辑。我们现在想复用这块加工逻辑，又不想用到CTE里emp表和dept表的关联。此时我们在使用这个CTE时，只查询出emp表的数据，就可以形成表裁剪。实际的执行逻辑只有对emp表的查询，没有join的处理。
# 表裁剪可以发生在CTE中
# 里层：CTE中进行emp表与dept表的join，用的是dept的主键字段进行join，此时可以使用emp的字段与dept的字段
# 外层：外层查询要复用CTE对emp的处理逻辑，只需要使用emp表的字段，形成表裁剪。此时实际执行计划中可去除对dept的join，只scan emp表。

# 下面是关闭表裁剪的执行计划。可以看到需要scan emp和dept表，再进行join。
# "- Output => [7:concat, 8:if]"
# "    - HASH/INNER JOIN [4:dept_id = 5:id] => [7:concat, 8:if]"
# "            Estimates: {row: 4, cpu: 305.33, memory: 16.00, network: 0.00, cost: 527.67}"
# "            7:concat := DictMapping(9: name, concat(emp_, 2: name))"
# "            8:if := if(3:sex = 1, '男', '女')"
# "        - SCAN [emp] => [3:sex, 4:dept_id, 9:name]"
# "                Estimates: {row: 6, cpu: 115.00, memory: 0.00, network: 0.00, cost: 57.50}"
# "                partitionRatio: 1/1, tabletRatio: 1/1"
#         - EXCHANGE(BROADCAST)
# "                Estimates: {row: 2, cpu: 48.00, memory: 48.00, network: 48.00, cost: 200.00}"
#             - SCAN [dept] => [5:id]
# "                    Estimates: {row: 2, cpu: 16.00, memory: 0.00, network: 0.00, cost: 8.00}"
# "                    partitionRatio: 1/1, tabletRatio: 1/1"
# 下面是开启表裁剪的执行计划。可以看到只需要scan emp表，没有join相关的处理，说明dept表被裁剪了。
# "- Output => [7:concat, 8:if]"
# "    - SCAN [emp] => [7:concat, 8:if]"
# "            Estimates: {row: 6, cpu: 230.00, memory: 0.00, network: 0.00, cost: 115.00}"
# "            partitionRatio: 1/1, tabletRatio: 1/1"
# "            7:concat := DictMapping(9: name, concat(emp_, 2: name))"
# "            8:if := if(3:sex = 1, '男', '女')"
explain
WITH
    t AS
        (
            SELECT
                concat('emp_', t1.name) busi_name,
                IF(t1.sex = 1, '男', '女') busi_sex,
                t2.name dept_name
            FROM
                mydb.emp t1
            LEFT JOIN mydb.dept t2
            ON
                t1.dept_id = t2.id
        )
SELECT busi_name, busi_sex FROM t;

# 表裁剪也可以发生在子查询中
# 里层：子查询中进行emp表与dept表的join，用的是dept的主键字段进行join，此时可以使用emp的字段与dept的字段
# 外层：外层查询要复用子查询中对emp的处理逻辑，只需要使用emp表的字段，形成表裁剪。此时实际执行计划中可去除对dept的join，只scan emp表。
explain logical
SELECT
    busi_name,
    busi_sex
FROM
    (
        SELECT
            concat('emp_', t1.name) busi_name,
            IF(t1.sex = 1, '男', '女') busi_sex,
            t2.name dept_name
        FROM
            mydb.emp t1
                INNER JOIN mydb.dept t2
                           ON
                               t1.dept_id = t2.id
    )
        t;

# 表裁剪也可以发生在逻辑视图中
# 里层：在逻辑视图中进行emp表与dept表的join，用的是dept的主键字段进行join，此时可以使用emp的字段与dept的字段
# 外层：使用这个视图时只查询emp表的字段，即可形成表裁剪。即使使用的是该视图，也不需要进行关联，只需要读取emp表的数据。
CREATE VIEW mydb.emp_dept_view AS
SELECT
    concat('emp_', t1.name) busi_name,
    IF(t1.sex = 1, '男', '女') busi_sex,
    t2.name dept_name
FROM
    mydb.emp t1
        LEFT JOIN mydb.dept t2
                  ON
                      t1.dept_id = t2.id;

explain select busi_name,busi_sex from mydb.emp_dept_view;

# 在inner join时，sr并不会对外键的正确性进行判断，也就是说不会判断外键字段的值，在被关联的表中是否存在，这需要由用户自己去保证。 因此就有可能导致裁剪后数据的不正确。
# 比如下面这个查询：
# dept表数据如下
# +--+------+
# |id|name  |
# +--+------+
# |1 |dept_1|
# |2 |dept_2|
# +--+------+
# emp表数据如下。可以看到，id为6的那条数据，外键dept_id的值在dept表是没有的。
# +--+---------+---+-------+
# |id|name     |sex|dept_id|
# +--+---------+---+-------+
# |6 |diu diu  |1  |3      |
# |1 |zhang san|1  |1      |
# |2 |li si    |2  |2      |
# |3 |wang wu  |1  |1      |
# |4 |zhao liu |2  |1      |
# |5 |duo duo  |1  |2      |
# +--+---------+---+-------+
# 如果不开启表裁剪，得到的结果如下。可以看到name='diu diu'的那条数据因为与dept匹配不上而被过滤了。
# +-------------+--------+
# |busi_name    |busi_sex|
# +-------------+--------+
# |emp_zhang san|男       |
# |emp_li si    |女       |
# |emp_wang wu  |男       |
# |emp_zhao liu |女       |
# |emp_duo duo  |男       |
# +-------------+--------+
# 但启动表裁剪后，实际得到的是6条数据，name='diu diu'的那条数据被查询出来了。这是因为开启了表裁剪，实际执行时只查询了emp表，没有进行关联，进而无法判断出是否能够匹配。
# +-------------+--------+
# |busi_name    |busi_sex|
# +-------------+--------+
# |emp_zhang san|男       |
# |emp_li si    |女       |
# |emp_wang wu  |男       |
# |emp_zhao liu |女       |
# |emp_duo duo  |男       |
# |emp_diu diu  |男       |
# +-------------+--------+
# 总结：
# 表裁剪需要依赖唯一约束或外键约束，而sr对明细模型的唯一约束或外键约束没有检查。因此如果实际数据不符合约束的要求，使用表裁剪时有可能造成【数据的不准确】。
SELECT
    busi_name,
    busi_sex
FROM
    (
        SELECT
            concat('emp_', t1.name) busi_name,
            IF(t1.sex = 1, '男', '女') busi_sex,
            t2.name dept_name
        FROM
            mydb.emp t1
                INNER JOIN mydb.dept t2
                           ON
                               t1.dept_id = t2.id
    )
        t;

update mydb.dept set province_id=10 where id=1;
update mydb.dept set province_id=20 where id=2;

select * from mydb.province;

insert into mydb.province values(10,'北京'),(20,'天津');

# 针对多表join的情况，可以将多张表设置为保留表。在这里可以看到，emp表和dept表被选为保留表（select中只列出了emp和dept的字段），province表则为裁剪表（select中没有列出province的字段）。
# 关闭表裁剪的执行计划。可以看到三个表都被scan了。
# "- Output => [10:concat, 6:name]"
# "    - HASH/LEFT OUTER JOIN [12:cast = 8:id] => [6:name, 10:concat]"
# "            Estimates: {row: 6, cpu: 342.00, memory: 16.00, network: 0.00, cost: 1053.00}"
# "            10:concat := DictMapping(13: name, concat(emp_, 2: name))"
# "        - HASH/LEFT OUTER JOIN [4:dept_id = 5:id] => [6:name, 12:cast, 13:name]"
# "                Estimates: {row: 6, cpu: 331.00, memory: 36.00, network: 0.00, cost: 564.50}"
#                 12:cast := cast(7:province_id as bigint(20))
#             - EXCHANGE(SHUFFLE) [4]
# "                    Estimates: {row: 6, cpu: 48.00, memory: 0.00, network: 48.00, cost: 227.50}"
# "                - SCAN [emp] => [4:dept_id, 13:name]"
# "                        Estimates: {row: 6, cpu: 91.00, memory: 0.00, network: 0.00, cost: 45.50}"
# "                        partitionRatio: 1/1, tabletRatio: 1/1"
#             - EXCHANGE(SHUFFLE) [5]
# "                    Estimates: {row: 2, cpu: 36.00, memory: 0.00, network: 36.00, cost: 90.00}"
# "                - SCAN [dept] => [5:id, 6:name, 7:province_id]"
# "                        Estimates: {row: 2, cpu: 36.00, memory: 0.00, network: 0.00, cost: 18.00}"
# "                        partitionRatio: 1/1, tabletRatio: 1/1"
#         - EXCHANGE(BROADCAST)
# "                Estimates: {row: 2, cpu: 48.00, memory: 48.00, network: 48.00, cost: 200.00}"
#             - SCAN [province] => [8:id]
# "                    Estimates: {row: 2, cpu: 16.00, memory: 0.00, network: 0.00, cost: 8.00}"
# "                    partitionRatio: 1/1, tabletRatio: 1/1"
# 启动表裁剪后的查询计划。可以看到province表被裁剪了。
# "- Output => [10:concat, 6:name]"
# "    - HASH/LEFT OUTER JOIN [4:dept_id = 5:id] => [6:name, 10:concat]"
# "            Estimates: {row: 6, cpu: 318.00, memory: 28.00, network: 0.00, cost: 524.67}"
# "            10:concat := DictMapping(13: name, concat(emp_, 2: name))"
#         - EXCHANGE(SHUFFLE) [4]
# "                Estimates: {row: 6, cpu: 48.00, memory: 0.00, network: 48.00, cost: 227.50}"
# "            - SCAN [emp] => [4:dept_id, 13:name]"
# "                    Estimates: {row: 6, cpu: 91.00, memory: 0.00, network: 0.00, cost: 45.50}"
# "                    partitionRatio: 1/1, tabletRatio: 1/1"
#         - EXCHANGE(SHUFFLE) [5]
# "                Estimates: {row: 2, cpu: 28.00, memory: 0.00, network: 28.00, cost: 70.00}"
# "            - SCAN [dept] => [5:id, 6:name]"
# "                    Estimates: {row: 2, cpu: 28.00, memory: 0.00, network: 0.00, cost: 14.00}"
# "                    partitionRatio: 1/1, tabletRatio: 1/1"
explain logical
SELECT
    busi_name,
    dept_name
FROM
    (
        SELECT
            concat('emp_', t1.name) busi_name,
            IF(t1.sex = 1, '男', '女') busi_sex,
            t2.name dept_name,
            t3.name province_name
        FROM
            mydb.emp t1
                LEFT JOIN mydb.dept t2
                          ON
                              t1.dept_id = t2.id
                LEFT JOIN mydb.province t3
                          ON
                              t2.province_id = t3.id
    )
        t