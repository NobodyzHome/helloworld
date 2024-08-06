drop table mydb.emp_info;
create table mydb.emp_info(
                              dt date,
                              emp_no varchar(50),
                              emp_name varchar(100),
                              dept_no varchar(50) not null,
                              dept_name varchar(100),
                              sex varchar(10),
                              salary int,
                              create_dt date not null,
                              work_days int
)
    primary key (dt,emp_no)
partition by date_trunc('day',dt)
distributed by hash(emp_no) buckets 2
order by(dept_no);

show tables;

LOAD LABEL load_emp_31
(
    DATA INFILE("hdfs://namenode:9000/data/employee")
    INTO TABLE emp_info
    COLUMNS TERMINATED BY ","
    ROWS TERMINATED BY "\n"
    FORMAT AS "CSV"
    -- 如果数据文件中字段顺序和表结构不一致，可以使用()来标明数据文件中第一个值对应数据表的哪个字段，第二个值对应数据表的哪个字段。
    -- 比如数据文件中内容是【emp_1,zhangsan,dept_1,职能部,1,2024-07-02,3000】,通过指定(emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary)，就把emp_1赋值给emp_no字段，zhangsan赋值给emp_name字段，dept_1赋值给dept_no字段，依此类推。
    (emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary)
    -- 我们可以使用set来生成新的字段,set语句必须放在()后边
    -- ()和set中提供的字段可以比表结构的字段多，不会影响导入，只不过多出来的字段不会导入到表中
    set(work_days=date_diff('day', curdate(), create_dt),dt=create_dt)
    -- 可以使用where语句来过滤要导入的数据
    where work_days>=30
 )
 WITH BROKER
PROPERTIES
(
    "timeout" = "72000"
);

LOAD LABEL load_emp_33
(
    DATA INFILE("hdfs://namenode:9000/data/employee")
    INTO TABLE emp_info
    -- 指定要导入的分区。注意：要导入的分区必须是已存在的，并且数据中要导入的分区也只能是指定的这个分区。
    -- 否则就会报这个错：[42000][1064] Unexpected exception: Unknown partition 'p20240528' in table 'emp_info'。
    partition(p20240526,p20240527)
    COLUMNS TERMINATED BY ","
    ROWS TERMINATED BY "\n"
    FORMAT AS "CSV"
    (emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary)
    set(work_days=date_diff('day', curdate(), create_dt),dt=create_dt)
    where dt in ('2024-05-26','2024-05-27')
 )
 WITH BROKER
PROPERTIES
(
    "timeout" = "72000"
);

# load_emp_28为本次load的label，可以使用该名称查询load任务的进度、取消load任务。
# 如果本次load成功了，那么下次load时就不能再使用该label了；如果本次load失败了，那么下次load时还可以使用该label。
# 注意：一次broker load是完整性事务的，也就是说一个load task即使一次性导入的文件再多，load完成之前，用户也看不到正在导入的数据。
load label load_emp_37(
     -- 导入的文件路径中可以使用*，能够匹配任何字符，通过此种方式可以一次性导入多个文件。例如【/dt=2024-06-27/*】能够匹配【/dt=2024-06-27/abc】、【/dt=2024-06-27/def】
     -- 注意：如果使用*进行通用匹配，务必保证最后的匹配结果是文件，而不是文件夹，否则会报错：type:ETL_RUN_FAIL; msg:No source file in this table(emp_info)。
     -- 例如配置的路径是/mydb.db/emp_info/*，那么只能匹配到/mydb.db/emp_info/dt=2024-06-27、/mydb.db/emp_info/dt=2024-06-28，这俩个路径都是文件夹，不是文件，就会报错。此时需要将配置的路径改成/mydb.db/emp_info/*/*，进一步匹配文件夹下的文件。
     data infile("hdfs://namenode:9000/user/hive/warehouse/mydb.db/emp_info/*/*")
     into table emp_info
     format as "ORC"
     (emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary)
     --  我们可以从文件的目录中提取出变量，比如我设置了变量dt，sr匹配到了文件hdfs://namenode:9000/user/hive/warehouse/mydb.db/emp_info/dt=2024-06-27/000000_0，那么会在该路径下找包含【dt=xxxx】的地方，找到的就是dt=2024-06-27，因此变量dt的值就是2024-06-27
     COLUMNS FROM PATH AS (dt)
     set(work_days=date_diff('day', curdate(), create_dt))
     --  对从目录提取到的变量进行过滤
     where dt>=curdate()-interval 60 day
 )
WITH BROKER
PROPERTIES
(
    "timeout" = "72000"
);

use mydb;
load label load_emp_14(
     data infile("hdfs://namenode:9000/user/hive/warehouse/mydb.db/emp_info/*/*")
     into table emp_info
     format as "ORC"
     (emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary)
     COLUMNS FROM PATH AS (dt)
     -- 我们可以在broker load中通过set方式增加__op字段，该字段用于指定当前数据是要在数据表中进行删除操作还是upsert操作。如果该字段值为0，则是upsert操作，如果该字段值为1，则是delete操作。默认没有设置__op参数的话，__op字段的值为0，代表upsert操作。
     -- 在这个例子中，我们将数据文件中dept_no为dept_0和dept_1的数据全都物理删除了。注意：如果导入的数据文件中，一条数据在数据表中不存在，但他的__op=1，那么这条数据不会保存到数据表中。
      set(work_days=date_diff('day', curdate(), create_dt),__op=if(dept_no in ('dept_0','dept_1'),1,0))
     where dt='2024-06-28'
)
WITH BROKER
PROPERTIES
(
     "timeout" = "120"
);

load label load_emp_13(
     data infile("hdfs://namenode:9000/data/employee_with_null_pk")
     into table emp_info
     -- 如果format是CSV，那么默认列的间隔符是\t，由于我们实际的数据中列是以,分割的，所以要将列间隔符设置成【,】
     COLUMNS TERMINATED BY ","
     -- 如果format是CSV，那么默认的行间隔是\n，由于我们实际的数据中行是以\n分割的，所以要将行间隔设置成【\n】
     ROWS TERMINATED BY "\n"
     -- 如果文件类型是CSV，那么一般需要设置列间隔符是什么、行间隔符是什么
     format as  "CSV"
     (emp_no_origin,emp_name,dept_no,dept_name,sex,create_dt,salary)
     set(work_days=date_diff('day', curdate(), create_dt),dt=create_dt,emp_no=if(emp_no_origin='null',null,emp_no_origin))
)
with broker
properties(
     -- 本次broker load任务的超时时长，单位是秒。当broker load任务执行时间超过该配置，则该broker load任务会变为【CANCEL】状态，不再继续执行。报错内容：type:LOAD_RUN_FAIL; msg:Load timeout. Increase the timeout and retry。
     -- 在这里故意将timeout设置很短，看任务超时后的处理方式
     "timeout"="1",
     -- 用于配置导入数据的容错率，当导入的数据中，错误的数据/导入的全部数据>max_filter_ratio，则本次导入失败，否则本次导入任务成功，只有正确的数据会导入进去，错误的数据不会导入进去。
     -- 举例来说，要导入的数据有10条，其中1条没有主键信息，导入主键字段为null，违反主键约束。此时错误率就是1/10=0.1，由于错误率(0.1)不大于max_filter_ratio的配置(0.1)，因此本次导入会成功，且只有9条数据导入进来。
     -- 如果没有配置max_filter_ratio，则max_filter_ratio=0，还以上例来说，此时错误率(0.1)大于max_filter_ratio的配置(0)，因此导入会失败，报错误：Error: NULL value in non-nullable column 'emp_no'. Row: [2024-06-08, NULL, '苍馥筠', 'dept_14', '运力部', 'female', 5849, 2024-06-08, 24, 0]。
     "max_filter_ratio" = "0.1"
);




select * from emp_info where dt='2024-06-27' and dept_no='dept_2';

# 把近期load任务都展示出来，不仅包含broker load，还包含insert等load task。
show load;
# 从sr 3.1起，我们也可以通过information_schema.loads视图来查询load任务的运行情况。相比show load的好处是，可以通过where条件查询关心的load task。
SELECT * FROM information_schema.loads where LABEL='load_emp_14';
# 通过cancel命令取消mydb库中label=load_emp_37的load task
CANCEL load from mydb where LABEL='load_emp_37';

truncate table emp_info;

show partitions from emp_info;
show temporary partitions from emp_info;

show tables;

# 给出一个label，进行问题排查
# 通过show load来查询指定label的执行记录，如果该load出现问题，TrackingSQL字段会有值。使用该sql进行查询，即可获取的load失败的原因。
# +-----+-------------------------------------------+--------+-------------------+------+--------+--------+------------+--------------+--------+-------+--------------------------------------------------+--------+-------------------+-------------------+-------------------+-------------------+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |JobId|Label                                      |State   |Progress           |Type  |Priority|ScanRows|FilteredRows|UnselectedRows|SinkRows|EtlInfo|TaskInfo                                          |ErrorMsg|CreateTime         |EtlStartTime       |EtlFinishTime      |LoadStartTime      |LoadFinishTime     |TrackingSQL|JobDetails                                                                                                                                                                                                                                                     |
# +-----+-------------------------------------------+--------+-------------------+------+--------+--------+------------+--------------+--------+-------+--------------------------------------------------+--------+-------------------+-------------------+-------------------+-------------------+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |14021|insert_0066744a-4e71-11ef-8296-0242ac120003|FINISHED|ETL:100%; LOAD:100%|INSERT|NORMAL  |0       |0           |0             |1       |null   |resource:N/A; timeout(s):300; max_filter_ratio:0.0|null    |2024-07-30 12:41:11|2024-07-30 12:41:11|2024-07-30 12:41:11|2024-07-30 12:41:11|2024-07-30 12:41:12|           |{"All backends":{"0066744a-4e71-11ef-8296-0242ac120003":[10004]},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":65,"InternalTableLoadRows":1,"ScanBytes":0,"ScanRows":0,"TaskNumber":1,"Unfinished backends":{"0066744a-4e71-11ef-8296-0242ac120003":[]}}|
# +-----+-------------------------------------------+--------+-------------------+------+--------+--------+------------+--------------+--------+-------+--------------------------------------------------+--------+-------------------+-------------------+-------------------+-------------------+-------------------+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
show load from mydb where  state <> 'FINISHED' label='insert_0066744a-4e71-11ef-8296-0242ac120003';
# 也可以使用information_schema.loads来查询指定label的执行情况
# +------+-------------------------------------------+-------------+--------+-------------------+------+--------+---------+-------------+---------------+---------+--------+--------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+------------+------------+--------------------+
# |JOB_ID|LABEL                                      |DATABASE_NAME|STATE   |PROGRESS           |TYPE  |PRIORITY|SCAN_ROWS|FILTERED_ROWS|UNSELECTED_ROWS|SINK_ROWS|ETL_INFO|TASK_INFO                                         |CREATE_TIME        |ETL_START_TIME     |ETL_FINISH_TIME    |LOAD_START_TIME    |LOAD_FINISH_TIME   |JOB_DETAILS                                                                                                                                                                                                                                                    |ERROR_MSG|TRACKING_URL|TRACKING_SQL|REJECTED_RECORD_PATH|
# +------+-------------------------------------------+-------------+--------+-------------------+------+--------+---------+-------------+---------------+---------+--------+--------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+------------+------------+--------------------+
# |14021 |insert_0066744a-4e71-11ef-8296-0242ac120003|mydb         |FINISHED|ETL:100%; LOAD:100%|INSERT|NORMAL  |0        |0            |0              |1        |        |resource:N/A; timeout(s):300; max_filter_ratio:0.0|2024-07-30 12:41:11|2024-07-30 12:41:11|2024-07-30 12:41:11|2024-07-30 12:41:11|2024-07-30 12:41:12|{"All backends":{"0066744a-4e71-11ef-8296-0242ac120003":[10004]},"FileNumber":0,"FileSize":0,"InternalTableLoadBytes":65,"InternalTableLoadRows":1,"ScanBytes":0,"ScanRows":0,"TaskNumber":1,"Unfinished backends":{"0066744a-4e71-11ef-8296-0242ac120003":[]}}|null     |null        |null        |null                |
# +------+-------------------------------------------+-------------+--------+-------------------+------+--------+---------+-------------+---------------+---------+--------+--------------------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------+------------+------------+--------------------+
select * from information_schema.loads where label='insert_0066744a-4e71-11ef-8296-0242ac120003';

# 排查近期执行失败的load
show load from mydb where  state = 'CANCELLED';
# 注意：show load方式针对state字段只能用等号进行限制，下面这个sql用不等于来执行，则会报错。
# [42000][1064] Getting analyzing error. Detail message: Where clause should looks like: LABEL = "your_load_label", or LABEL LIKE "matcher", or STATE = "PENDING|ETL|LOADING|FINISHED|CANCELLED|QUEUEING", or compound predicate with operator AND.
show load from mydb where  state <> 'FINISHED';
# 相比show load，使用information_schema.loads则更灵活，可以使用不等于
select * from information_schema.loads where state <> 'FINISHED';

drop table emp_info;


select * from emp_info where work_days>30;
explain analyze select dt,count(*) from emp_info group by dt;

show partitions from emp_info;

select tracking_log from information_schema.load_tracking_logs where job_id=12052;
