-- zhangsan,emp_1,2024-06-15 13:20:10,1100,20
-- lisi,emp_2,2024-06-15 16:20:10,2000,15
-- wangwu,emp_3,2024-06-15 16:20:10,2200,16
-- zhaoliu,emp_4,2024-06-15 17:20:10,3000,17
-- leilei,emp_5,2024-06-15 19:20:10,3100,18
-- dada,emp_6,2024-06-15 21:20:10,3300,19
-- xixi,emp_7,2024-06-15 22:20:10,3100,18
-- xiaoming,emp_8,2024-06-15 23:20:10,5000,21
-- xiaoli,emp_9,2024-06-15 21:20:10,2300,23

use mydb;
show tables ;
drop table mydb.emp_info_routine_load;
create table mydb.emp_info_routine_load(
    emp_no varchar(100),
    dt date,
    emp_name varchar(200),
    age int,
    salary int not null,
    create_tm datetime
)
primary key(emp_no,dt,emp_name)
partition by (dt)
distributed by hash(emp_name)
order by (salary);


create routine load mydb.routine_load_emp_4 on emp_info_routine_load
COLUMNS TERMINATED BY ",",
-- COLUMNS和broker load的()一样，主要是用来指定，当数据源中字段顺序和表结构顺序不一致时，数据源中的字段和表结构中字段的对应关系。
-- 比如数据是这样的【zhangsan,emp_1,2024-06-15 13:20:10,1100,20】，通过COLUMNS (emp_name,emp_no,create_tm,salary,age)指定zhangsan对应emp_name字段，emp_1对应emp_no字段，依次类推
-- 相比broker load，衍生字段可以直接放在COLUMNS里，而不用放在SET语句里
COLUMNS (emp_name,emp_no,create_tm,salary,age,dt=cast(create_tm as date))
PROPERTIES(
    "desired_concurrent_number"="5",
    "format" = "csv"
)
from kafka(
    -- 配置kafka的broker
     "kafka_broker_list" = "kafka-1:9092",
    -- 配置拉取的topic
    "kafka_topic" = "hello_starrocks",
    -- 配置拉取的partition
    "kafka_partitions" = "0,1,2",
    -- 控制读取的位点，目前可选值为：OFFSET_BEGINNING、OFFSET_END、具体offset
    -- 目前不能从group id中获取位点，所以这块不太好。如果一个Routine Load被停止了，再启动一个新的Routine Load任务，只能通过指定位点的方式继续处理，无法自动从group id中获取需要读取的位点。
    -- 因此一个Routine Load不要轻易stop，如果暂时不消费了的话，可以暂停任务，而不是停止任务。
    "property.kafka_default_offsets" = "OFFSET_BEGINNING",
    -- 配置消费kafka的group id
    "property.group.id"="routine_group"
);

# {"creat_time":"2024-06-15 15:20:10","e_salary":3310,"e_name":"qiqi","e_no":"emp_11","e_age":15}
#  {"e_age":19,"e_salary":2100,"creat_time":"2024-06-15 15:20:10","e_name":"toto","e_no":"emp_12"}
#  {"e_age":32,"e_salary":3300,"creat_time":"2024-06-15 16:20:10","e_name":"zhangsan","e_no":"emp_1"}
# {"e_age":21,"e_salary":3300,"creat_time":"2024-06-15 16:20:10","e_name":"wangwu","e_no":"emp_3"}
# {"e_age":19,"e_salary":2100,"creat_time":"2024-06-15 16:30:35","e_name":"lisi","e_no":"emp_2"}
# {"e_age":17,"e_salary":5500,"creat_time":"2024-06-15 16:30:35","e_name":"lisi","e_no":"emp_2"}

create routine load mydb.routine_load_emp_7 on emp_info_routine_load
COLUMNS (emp_name,emp_no,create_tm,salary,age,dt=cast(create_tm as date),__op=if(age>20,1,0))
properties(
    "format"="json",
    -- jsonpaths和COLUMNS中相同位置的映射在一起
    -- jsonpaths=[\"$.e_name\",\"$.e_no\",\"$.creat_time\",\"$.em_salary\",\"$.e_age\"]
    -- columns (emp_name,emp_no,create_tm,salary,age)
    -- 这种情况下，数据中e_name字段的值会赋值到emp_name（都是在第一位），数据中e_no字段的值会赋值到emp_no（都是在第二位），依此类推
    -- 注意：jsonpaths中给出的顺序不需要和数据中json的key的顺序相同，只需要给出的key和数据中的key相同即可
    "jsonpaths"="[\"$.e_name\",\"$.e_no\",\"$.creat_time\",\"$.e_salary\",\"$.e_age\"]"
)
from kafka(
    "kafka_broker_list"="kafka-1:9092",
    "kafka_topic"="hello_starrocks_json",
    "kafka_partitions"="0,1,2,3",
    "property.kafka_default_offsets"="OFFSET_END"
);

show partitions from emp_info_routine_load;

-- 查询指定的routine load job
show routine load for routine_load_emp_7;
-- 查询指定的routine load job下的load task
show routine load  TASK where jobName= 'routine_load_emp_7';

select * from emp_info_routine_load;

stop routine load for mydb.routine_load_emp_5;

pause routine load for mydb.routine_load_emp_4;

resume routine load for mydb.routine_load_emp_4;

select tracking_log from information_schema.load_tracking_logs where job_id=12238;

truncate table emp_info_routine_load;

select tracking_log from information_schema.load_tracking_logs where job_id=12240;


SHOW RESOURCE GROUPS;

SHOW RESOURCE GROUPS ALL;


SHOW USAGE RESOURCE GROUPS;

SHOW USAGE RESOURCE GROUPS;

SET enable_pipeline_engine = true;

SET GLOBAL enable_pipeline_engine = true;

CREATE RESOURCE GROUP group_name
TO (
    query_type in ('select')
) -- 创建分类器，多个分类器间用英文逗号（,）分隔。
WITH (
    "cpu_core_limit" = "1",
    "mem_limit" = "20%"
);

explain verbose select * from mydb.base_table1;

SHOW RUNNING QUERIES;