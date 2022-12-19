create table hive_test(
    id int,
    name string,
    age int
)
partitioned by (row_date date)
row format delimited
fields terminated by ',';

create table hello_hive_multi_partition(id int ,name string) partitioned by(row_time date,age int) row format delimited fields terminated by ',';


insert into hive_test partition(row_date='2022-03-01') values(1,'zhangsan',20),(2,'lisi',10);

-- 创建分区表
create table package_state(waybill_code string,waybill_type int,vendor_id string) partitioned by(year int,month int,day int);
-- 向指标分区写入数据
insert into package_state partition(year=2022,month=3,day=15) values("JDA",12,"1001"),("JDB",13,"1002"),("JDC",12,"1003"),("JDD",211,"1004"),("JDA",11,"1002");
insert into package_state partition(year=2022,month=3,day=16) values("JDA",13,"1001"),("JDB",15,"1009"),("JDC",16,"1004"),("JDE",122,"1003"),("JDF",20,"112");
insert into package_state partition(year=2022,month=3,day=17) values("JDA",19,"1001"),("JDB",221,"100X");
insert into package_state partition(year=2022,month=3,day=11) values("JDA",19,"100Y"),("JDB",221,"100Z");
insert into package_state partition(year=2022,month=4,day=22) values("JDC",15,"1002"),("JDE",221,"10XX"),("JDA",11,"1099"),("JDM",1112,"1"),("JDB",192,"107"),("JDC",15,"1020");
insert into package_state partition(year=2022,month=4,day=15) values("JDI",16,"11"),("JDK",12,"16"),("JDA",15,"1099");
insert into package_state partition(year=2022,month=4,day=22) values("JDO",18,"13"),("JDB",13,"11"),("JDX",17,"12");
insert into package_state partition(year=2022,month=4,day=26) values("JDI",15,"13"),("JDE",22,"18"),("JDT",17,"87");
insert into package_state partition(year=2022,month=4,day=27) values("JDU",15,"17"),("JDR",22,"22"),("JDW",17,"31");

create table waybill_route_link(
    waybill_code string,
    operate_type int,
    operator string
)
partitioned by (dp string,dt string)
row format delimited
fields terminated by ',';

insert into waybill_route_link partition(dp='ZACTIVE',dt='2022-03-05') values("JDA",10,"ZHANG_SAN"),("JDB",20,"LI_SI"),("JDC",30,"LAO_LIU"),("JDD",40,"ZHAO_WU");
insert into waybill_route_link partition(dp='ZACTIVE',dt='2022-03-06') values("JDE",15,"XX"),("JDB",30,"YY"),("JDF",10,"LL"),("JDG",46,"QO_QO");
insert into waybill_route_link partition(dp='ZACTIVE',dt='2022-04-11') values("JDI",20,"MM"),("JDC",40,"II"),("JDP",10,"ZZ"),("JDD",50,"UU");
insert into waybill_route_link partition(dp='ZACTIVE',dt='2022-04-12') values("JDN",33,"EE"),("JDV",66,"PP");
insert into waybill_route_link partition(dp='ZACTIVE',dt='2022-04-13') values("JDY",43,"KK"),("JDQ",90,"OO");

insert into waybill_route_link partition(dp='HISTORY',dt='2022-03-05') values("JDA_H",10,"ZHANG_SAN"),("JDB_H",20,"LI_SI"),("JDC_H",30,"LAO_LIU"),("JDD_H",40,"ZHAO_WU");
insert into waybill_route_link partition(dp='HISTORY',dt='2022-03-06') values("JDE_H",15,"XX"),("JDB_H",30,"YY"),("JDF_H",10,"LL"),("JDG_H",46,"QO_QO");
insert into waybill_route_link partition(dp='HISTORY',dt='2022-04-11') values("JDI_H",20,"MM"),("JDC_H",40,"II"),("JDP_H",10,"ZZ"),("JDD_H",50,"UU");
insert into waybill_route_link partition(dp='HISTORY',dt='2022-04-11') values("JDI_H",50,"ii"),("JDC_H",60,"vv");

insert into waybill_route_link partition(dp='ACTIVE',dt='2022-03-05') values("JDA_A",10,"ZHANG_SAN"),("JDB_A",20,"LI_SI"),("JDC_A",30,"LAO_LIU"),("JDD_A",40,"ZHAO_WU");
insert into waybill_route_link partition(dp='ACTIVE',dt='2022-03-06') values("JDE_A",15,"XX"),("JDB_A",30,"YY"),("JDF_A",10,"LL"),("JDG_A",46,"QO_QO");
-- 以添加新文件的方式，将数据写入对应的分区
insert into waybill_route_link partition(dp='ACTIVE',dt='2022-04-11') values("JDI_A",20,"MM"),("JDC_A",40,"II"),("JDP_A",10,"ZZ"),("JDD_A",50,"UU");
-- 以覆盖方式写入数据，该分区之前的数据会被清除，不会影响到其他分区的数据
insert overwrite table summary_test select name,count(*) from student group by name;

create table teacher (id int,name string,sex int) partitioned by(dp string,dt date) row format delimited fields terminated by ',';
insert into teacher partition(dp='active',dt='2022-03-15') values(1,'lisi',1),(2,'zhangliu',2),(3,'zhangsan',1),(4,'wangwu',2),(5,'lisi',2);

create external table external_test (id int,name string) partitioned by (dp string,dt string) row format delimited fields terminated by ','  location '/test/partitioned/external';
insert into external_test partition(dp='active',dt='2022-03-15') values(3,'coco'),(4,'lala');
create external table external_table (id int,name string)  row format delimited fields terminated by ','  location '/test/external';

select TIMESTAMPDIFF(DAY,to_date('2022-03-08 15:33:22.0','yyyy-MM-dd'),CURRENT_DATE);


create table employee(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
row format delimited fields terminated by ',' lines terminated by '\n'
stored as textfile;

-- load data用于将指定的数据文件发送到hive table对应的hdfs目录，load会做两件事：
-- 1.如果inpath是本地文件，那么将文件上传到hive表或分区的目录（hdfs -fs -put）；如果inpath是hdfs路径，则将指定路径的文件移动到hive表或分区的目录（hdfs -fs -mv）
-- 2.如果metastore里没有要加载的分区信息，则在metastore的partitions表中创建该分区的信息
load data inpath '/upload/emp_data.txt' overwrite into table employee;

-- over内没有任何内容，那么每条数据的窗口大小是一样的，即窗口内的数据内容都是全表数据
-- 如果over内既没有order by，也没有rows限定条件，那么会默认增加rows between unbounded preceding and unbounded following，即取窗口基础数据的所有数据作为窗口的实际数据
select *,sum(salary) over() from employee;
-- 当over内只有partition by dept_no时，代表窗口中的内容按照当前数据中的dept_no的值进行过滤，因此相同dept_no值的数据的窗口数据内容是相同的。例如当前数据dept_no=hr，那么该数据的窗口中的数据内容是数据源中所有dept_no=hr的数据。
select *,sum(salary) over(partition by dept_no) from employee;
-- over(order by salary)，由于没有给出range限制条件，因此会增加一个默认的rows限定条件，即增加rows between unbounded preceding and current row条件。
-- over(order by salary)，由于没有partition by，因此窗口中基础数据内容是全表中的数据。同时没有rows限定条件，默认增加了rows between unbounded preceding and current row，因此每条数据的窗口的实际内容是：全表数据按照salary排序后，取第一条数据到当前数据。
select *,sum(salary) over(order by salary) from employee;
-- over(partition by dept_no order by salary)，既有partition by，又有order by。那么每条数据的窗口的内容是：过滤出全表中dept_no为当前数据dept_no值的数据，按salary进行排序，取排序后的第一条数据到当前数据。
select *,sum(salary) over(partition by dept_no order by salary) from employee;
-- over(order by salary rows between 2 preceding and 3 following)，每条数据的窗口是：以全表数据为基础数据，按salary进行排序，取当前数据的前两条、当前数据、当前数据的后三条作为窗口的实际数据内容。
select *,sum(salary) over(order by salary rows between 2 preceding and 3 following) from employee;
-- over(partition by sex order by salary rows between current row and 4 following)，每条数据的窗口数据内容是：以全表中sex为当前数据sex值的数据作为基础数据，按salary进行排序，取当前数据在基础数据的位置到窗口最后一条数据，作为窗口的实际数据内容。
-- 总结,over内主要由三部分组成：
-- 1.【确定窗口数据基础范围】partition by：使用当前数据的对应字段的值，过滤出窗口中数据的基础内容。如果没有写，则代表基础内容是全表中的数据
-- 2.【确定当前数据在窗口基础范围的位置】order by：对窗口基础数据内容进行排序，找出当前数据的内容在窗口基础数据中的位置。如果没有给出rows，默认增加rows between unbounded preceding and current row
-- 3.【根据数据在窗口基础范围的位置来挑选窗口实际的数据内容】rows between x preceding and y following：在窗口基础数据排序后，按照指定的要求和当前数据在窗口的位置，挑选出窗口实际的数据内容。每条数据的窗口的内容是：当前数据的前x条、当前数据、当前数据的后y条。
select *,sum(salary) over(partition by sex order by salary rows between current row and unbounded following) from employee;

select id,collect_list(id) over(order by id) window_elements,sum(id) over(order by id) sum_id from test1;

select *,RANK() over(order by dept_no) from employee;
select *,DENSE_RANK() over(order by dept_no) from employee;

set mapreduce.job.reduces=3;
-- order by用于全数据排序，在map-reduce中，会强制让任务只有一个reducer，这样所有map都将数据发送给同一个reducer，这样这个reducer收到的自然就是全数据，就可以进行全数据的排序了。由于这种方式只能有一个reducer，因此运行效率非常低
insert overwrite table test select * from employee order by salary desc;
-- distribute by dept_no sort by salary 按照dept_no进行分区，然后每个分区中再按salary进行排序，即保证分区内有序。注意，有可能是多个dept_no的值放在同一个分区，因此每个分区内是对多个dept_no值的数据按dept_no、salary进行排序。
insert overwrite table test select * from employee distribute by dept_no sort by dept_no,salary;
-- sort by也是能够保证分区内有序，但是sort by有个问题就是不知道他是按什么分区的，每条数据应该分到哪个分区是随机。这往往不符合我们的需求，因为一堆不相干的数据放在一个分区然后再排序，这是没有意义的。
-- 所以我们一般在使用sort by前给出distribute by，指定拥有相同字段的值分到一个分区里，再对这个分区的数据排序才是有意义的。
insert overwrite table test select * from employee sort by salary desc;
-- cluster by dept_no 相当于distribute by dept_no sort by dept_no，但是cluster by不支持desc
insert overwrite table test select * from employee cluster by dept_no;

create table waybill_route_link(
    waybill_code string,
    operate_type int,
    operate_site int,
    operate_site_name string,
    route string
)
-- 增加分区字段
partitioned by (dt string)
-- 数据文件中数据的样式
row format DELIMITED fields terminated by '#' lines terminated by '\n'
-- 数据文件存储格式
stored as textfile;

-- 使用create table as select的方式，只能是创建一个非分区表，然后将select中的数据（select的数据表可以是分区表）写入这个非分区表。
-- 下面这个语句尝试create一个分区表，执行时会报以下异常：CREATE-TABLE-AS-SELECT does not support partitioning in the target table
create table waybill_route_link_bak
    partitioned by (dt string)
row format DELIMITED fields terminated by '@' lines terminated by '\n'
stored as textfile
as
select
    *
from waybill_route_link;

-- 使用create table as select的方式，不能指定table中的字段。这个table有哪些字段完全是由select语句推断出来，如果是select *，那么会把select表的分区字段放在数据字段的最后，作为新表的一个字段
-- 使用create table as select的方式，可以指定row format、stored as等，这些可以与select的table不一样
create table waybill_route_link_bak
-- 给出了和数据源表不同的数据存储格式
stored as orc
as
select
    waybill_code,
    operate_type,
    operate_site_name,
    route
from waybill_route_link
where
    waybill_code is not null;

-- 创建一个table，其表定义完全和like的表一样
create table waybill_route_link1 like waybill_route_link_bak;

-- 删除一个表，会进行以下操作：
-- 1.在metastore的tbls、partitions表中删除该表的数据
-- 2-1.如果该表是管理表，则会直接在hdfs删除该table的目录及目录下的内容
-- 2-2.如果该表是外部表，则不会删除该表在hdfs中的目录
drop table waybill_route_link_bak;

insert into waybill_route_link partition(dt='2022-09-09') values('JDA',21,10013,'xian_site','beijing_sort,beijing_site,xian_sort,xian_site'),('JDB',21,101,'shanxi_site','xian_sort,xian_site,shanxi_site'),('JDC',15,1101,'hainan',null);

-- 清空一个表的所有数据，会进行以下操作：
-- 1.去metastore中查询partitions表，获取该表的所有分区
-- 2.删除每一个分区目录下的所有数据文件
-- 注意：truncate只会删除该表下的所有数据文件，不会删除分区
-- 如果truncate不是分区表，则直接删除该表在hdfs目录下的所有文件
truncate table waybill_route_link;

-- 在使用truncate时还可以指定分区，仅删除某一个分区目录下的所有文件
truncate table waybill_route_link partition (dt='2022-09-08');

-- 侧表的实现思路是：
-- 1.拿主表的字段去调侧表的udtf
-- 2.udtf可能会返回多条数据，侧表每返回一条数据，就和主表数据拼接成一条数据。因此如果侧表调完udtf后返回N条数据，那么主表该数据就会被增加成N条数据
-- 当加上侧表后，select * 中就包含my_col字段了
select * from waybill_route_link lateral view explode(split(route,',')) my_table as my_col;

-- 如果lateral view后没有增加outer，那么如果当前数据调用udtf后，udtf没有返回任何数据，当前数据就会被过滤掉。而如果lateral view后面有outer，那么即使udtf没有返回数据，当前数据也不会被过滤掉。
select * from waybill_route_link lateral view outer explode(split(route,',')) my_table as my_col;

-- 不能将侧表出来的数据再作为子查询的数据源，下面sql会报错：FAILED: ParseException line 4:5 cannot recognize input near '(' 'select' 'waybill_code' in joinSource
select
    waybill_code,operate_type,route_site
from (
     select waybill_code,operate_type,route_site from waybill_route_link lateral view explode(split(route,',')) route_table as route_site
 );

-- 如果对侧表出来的数据进行group by，那么是使用下面的方式，不能将侧表出来的数据作为子查询的数据源
select route_site,count(waybill_code) cnt from waybill_route_link lateral view explode(split(route,',')) route_table as route_site group by route_site;

-- 需求是：一个运单的route字段中有多个站点，需要先拆成列转行，变成多条数据。然后求当前运单当前站点的数据的上游站点和下游站点
-- 例如一条是这样的：JDA,'beijing_sort,beijing_site,shanxi_site'
-- 需要先列转行成以下数据
-- JDA,beijing_sort
-- JDA,beijing_site
-- JDA,shanxi_site
-- 然后为每条数据找到上游站点和下游站点，形成以下数据
-- JDA,beijing_sort,null,beijing_site
-- JDA,beijing_site,beijing_sort,shanxi_site
-- JDA,shanxi_site,beijing_site,null
select
    *,
    lead(route_site) over(partition by waybill_code) site_downstream,
    lag(route_site) over(partition by waybill_code) site_upstream
from waybill_route_link lateral view outer explode(split(route,',')) t as route_site;

-- 直接使用udtf，这样出来的数据中只能有一个字段，就是udtf返回的字段
select explode(split(route,',')) from waybill_route_link;

select
    waybill_code,
    route,
    route_site,
    lead(route_site) over (partition by waybill_code) downstream,
   count(1) over(partition by waybill_code) cnt,
   count(1) over(partition by route_site) cnt_1
from waybill_route_link lateral view outer explode(split(route,',')) t as route_site;

select route_site,count(*) cnt
from waybill_route_link lateral view explode(split(route,',')) t as route_site
group by route_site;

select * from employee where dept_no is null;
insert into employee values('emp_999','测试',null,null,'male',null,1000);

select * from employee;
select
    *,
    avg(salary) over(partition by sex order by salary rows between 1 preceding and 1 following) near_avg_salary,
    avg(salary) over(partition by sex) avg_salary
from employee;

desc function extended split;

select
    emp_name,
    salary,
    word,
    count(1) over(partition by word) cnt
from employee lateral view explode(split(emp_name,'')) lt as word
where
    word is not null and word <> ''
order by word;

-- 查询所有分区
show partitions waybill_route_link;
-- 查询指定分区
show partitions waybill_route_link partition(dt='2022-09-09');

-- hive大部分ddl都会操作两部分：
-- 1.hdfs对应的文件夹或文件
-- 2.metastore中对应的元数据表
-- 因此add partition操作会：
-- 1.向hdfs中该表的路径下创建一个分区文件夹
-- 2.向metastore的partitions表中插入一条数据
alter table waybill_route_link add partition (dt='2022-09-09') partition(dt='2022-09-08');

-- drop partition操作会：
-- 1.删除hdfs中该分区对应的文件和目录
-- 2.删除metastore中partitions表对应的数据
alter table waybill_route_link drop partition (dt='2022-09-09'),partition(dt='2022-09-08');

show partitions waybill_route_link;

-- 根据hive表的hdfs的分区目录来修复metastore中partitions表的数据
MSCK REPAIR TABLE waybill_route_link;

-- load命令在操作hdfs命令的同时，还会操作元数据。因此将文件load到一个不存在的partition时，load命令除了将inpath指定的文件移动到指定分区的目录，还会在partitions表中增加一条该分区的数据
load data inpath '/user/hive/warehouse/waybill_route_link/dt=2022-09-15/000000_0' into table waybill_route_link partition(dt='2022-09-10');

show functions like 'a*';

desc function extended abs;

describe extended waybill_route_link;

show create table waybill_route_link;

show partitions waybill_route_link;

describe waybill_route_link partition (dt='2022-09-07');

describe  waybill_route_link;


-- insert values
-- 1.insert values 静态分区
-- 通过在表后面用partition来表示values中的数据要插入到哪个固定的分区
insert into waybill_route_link partition (dt='2022-09-20') values('JDD',21,10013,'xian_site','beijing_sort,beijing_site,xian_sort,xian_site'),('JDF',21,111,'beijing_site','beijing_sort,beijing_site,xian_sort,xian_site');
-- 2.insert values 动态分区
-- 如果使用动态分区，需要将hive.exec.dynamic.partition.mode参数设为nonstrict，设为strict时，在插入动态分区时，需要有一个分区字段是静态的（例如有dp、dt二级分区，如果动态分区的话，那么insert时，dp分区字段的值必须是静态的）
-- 注意：插入动态分区时，分区字段默认是最后一个字段，别赋值错了
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table waybill_route_link partition(dt) values('JDD',21,10013,'xian_site','beijing_sort,beijing_site,xian_sort,xian_site','2022-08-11'),('JDE',30,125,'shanxi_site','beijing_sort,beijing_site,xian_sort,xian_site','2022-08-16');
insert into waybill_route_link partition(dt='2022-08-11') values('JDG',21,10013,'xian_site','beijing_sort,beijing_site,xian_sort,xian_site'),('JDN',30,125,'shanxi_site','beijing_sort,beijing_site,xian_sort,xian_site');


create table waybill_route_link_copy like waybill_route_link;
-- insert from querys
-- 1.insert from query 静态分区
insert overwrite table waybill_route_link_copy partition (dt='history')
select
    waybill_code,
    operate_type,
    operate_site,
    operate_site_name,
    route
from waybill_route_link;
-- 2.insert from query 动态分区
set hive.exec.dynamic.partition.mode=nonstrict;
insert into waybill_route_link_copy partition (dt)
select * from waybill_route_link where dt>='2022-09-01';

/*
    在hive中，一个表分为两部分：一是存在于metastore的元数据，存储了这个表有哪些字段、有哪些分区、数据存储位置等信息。二是存在于hdfs中的真正的数据文件。
    hive认为管理表同时拥有这两部分的权限，即可以增加字段、分区，也可以增加或删除hdfs中的数据文件(Hive assumes that it owns the data for managed tables,data is attached to the Hive entities. So, whenever you change an entity (e.g. drop a table) the data is also changed (in this case the data is deleted))
    hive认为外部表只拥有元数据的权限，即只能增加字段、分区，不能对数据文件进行修改(For external tables Hive assumes that it does not manage the data.Use external tables when files are already present or in remote locations, and the files should remain even if the table is dropped.)
    我们一般把管理表就看作关系型数据库的表，他能够管理数据。而外部表的使用场景是：假设已经有了一批同构的数据文件（例如通过数据采集抓取来的），我们建立一个外部表，并将location指向对应数据文件的目录，然后我们就可以通过对外部表执行聚合等语句来达到对那些数据文件进行分析的目的。
    总结：
    1.管理表真正用于管理数据，他的生命周期是和数据文件同步的。给表增加数据，就会在对应目录下增加数据文件；当表被drop后，元数据和数据文件都被删除了。当然，管理表管理数据的最终目的，也是为了使用sql语句对这些数据进行分析。
    2.外部表相当于在已有的数据文件上盖上一层表，目的就是以访问表的方式来对已有数据文件进行分析。他的生命周期是和数据文件独立开来的，当外部表被删除后（也就是元数据中该表被删除），对应的数据文件也不会被删除
 */
create external table  hello_external(
    id int,
    name string
)
-- 外部表也可以是分区表，意味着外部数据采集软件，是按天rollup的。例如10号采集的数据放在2022-09-10文件夹中，11号采集的数据放在2022-09-11文件夹中
partitioned by (dt string)
-- 这里设置的数据样式必须和外部数据文件的样式能够匹配上，否则无法将数据解析成对应的字段
row format delimited fields terminated by ',' lines terminated by '\n'
-- 使用外部表时，可以自己指定表的路径。假设我们是先有数据文件后有表的话，我们在建外部表时，就需要把location设置为数据文件所在的路径
location '/hello_external';

-- 我们可以向外部表插入数据。不过尽量不要这样，我们建立外部表，是想要通过hive来加工已有的数据，而非往里新增数据
insert into hello_external partition(dt) values(1,'hello','2022-09-10'),(2,'external','2022-09-11');
-- 不能删除外部表的数据，因为hive认为外部表只有使用数据的权利，没有删除数据的权利。如果对外部表进行truncate操作，会报错误：[Error 10146]: Cannot truncate non-managed table hello_external.
truncate table hello_external;
-- 当外部表被drop后，只会在metastore中删除该表的元数据，不会删除对应的数据文件
drop table hello_external;
-- 可以通过describe formatted的数据结果中的Table Type字段来获取当前表是管理表还是外部表
describe formatted hello_external;
-- 通过对外部表使用sql来达到对外部数据进行分析的目的
select ch,count(*) word_cnt from hello_external lateral view explode(split(name,'')) t as ch
where ch <> ''
group by ch;



-- 下面四行执行完毕后，就得到了一个分区目录下有文件，但元数据中没有所有分区信息的表
create external table partition_table like waybill_route_link;
insert into partition_table partition(dt) select * from waybill_route_link;
drop table partition_table;
create external table partition_table like waybill_route_link;
-- 当我们自己手动在hdfs中建了分区目录和数据文件，但是元数据中partitions表还没有分区时，我们是查不到数据的。因为hive查分区表，永远是先去partitions表中查询分区信息，由于没有查到分区信息，因此不会加载分区目录下的数据文件。
select * from partition_table;
-- 我们手动在元数据中增加了一个分区的信息，我们就可以查到该分区的数据了
alter table partition_table add partition (dt='2022-08-11');
select * from partition_table where dt='2022-08-11';
-- 我们通过msck命令，根据hdfs中的分区目录来修复元数据，将分区目录已存在但是元数据中没有的分区都添加到partitions表中
msck repair table partition_table;
-- 元数据中分区修复完毕后，我们再直接查该表，就可以查到所有分区的数据了
select * from partition_table;


create table two_partition(
    name string
)
partitioned by (dp string,dt string)
row format delimited fields terminated by '@' lines terminated by '\n'
stored as textfile;

insert into two_partition partition (dp,dt) values('ACTIVE-2022-09-11','ACTIVE','2022-09-11'),('ACTIVE-2022-09-10','ACTIVE','2022-09-10'),('ACTIVE-2022-09-09','ACTIVE','2022-09-09'),('HISTORY-2022-09-11','HISTORY','2022-09-11'),('HISTORY-2022-09-10','HISTORY','2022-09-10'),('HISTORY-2022-09-09','HISTORY','2022-09-09');
// 全表查询
select * from two_partition;
// 只按照一级分区查询
select * from two_partition where dp='HISTORY';
// 按照一级和二级分区查询
select * from two_partition where dp='ACTIVE' and dt='2022-09-11';
// 只按照二级分区查询
select * from two_partition where dt='2022-09-11';

insert into waybill_route_link partition(dt='2021-09-30') values('JGBB',15,1911,'xian_site','beijing_sort,beijing_site,xian_sort,xian_site');

alter table waybill_route_link add columns(
    yn int comment '是否有效'
)
-- 加上cascade后，不仅对table的元数据加上该字段，还会对所有partition的元数据加上该字段
cascade;

alter table waybill_route_link add columns(
    -- 由于没有加cascade，因此仅会对table的元数据加上该字段，也就是说历史partition中没有该字段，而以后新建的partition中有该字段
    -- 如果历史partition的元数据不加该字段，那么查询历史分区，该字段的值永远为null，并且就算insert插入数据时给历史分区的该字段插入值了，在查询时，该字段的值也为null。
    -- 但是，实际插入的文件中，该字段是有值的，只是查询时查询不出来，就是因为该partition的元数据没有该字段
    version int comment '数据版本'
);

show partitions waybill_route_link;
describe waybill_route_link partition (dt='2022-08-11');

select * from waybill_route_link where dt='2021-09-30';
insert into waybill_route_link partition(dt='2021-09-30') values ('JDOO',75,187,'xian_site','beijing_sort,beijing_site,xian_sort,xian_site',0,10);

insert into waybill_route_link partition(dt='2021-01-30') values('MZ',23,1911,'xian_site','beijing_sort,beijing_site,xian_sort,xian_site',2,5);
select * from waybill_route_link where dt='2022-08-11';

create table waybill_route_link_replace
row format delimited fields terminated by '#' lines terminated by '\n'
stored as textfile
as
select
    *
from waybill_route_link;

drop table waybill_route_link_replace;
select * from waybill_route_link_replace;

-- replace用于使用新字段列表完全替代已有的字段列表。注意，该命令只是修改元数据，是不会修改数据文件的。
-- 假设原表有四个字段waybill_code,operate_type,operate_site,site_name，对应数据文件内容为JDV,12,1311,xian_site。当使用replace变为两个字段way_bill,op_type后，该表查询的内容为JDV,12。但是数据文件的内容没有被修改
-- 注意相同位置的字段在replace前后的类型是否兼容，例如replace前，第三个字段是int，replace后，第三个字段是string，int -> string是可以兼容的，不会报错。但是如果第二个字段replace前是string，replace后是int，string -> int是不兼容的，则会报出异常：Unable to alter table. The following columns have types incompatible with the existing columns in their respective positions :site_name
alter table waybill_route_link_replace replace columns(
    waybill_code string,
    operate_type string,
    operate_site string,
    site_name string
);

select * from waybill_route_link_replace;
describe formatted waybill_route_link_replace;

-- 使用change可以更改指定字段的名字、类型、注释等。也是用cascade来对所有partition的元数据进行修改，不加cascade则只对table的元数据进行更改
alter table waybill_route_link change operate_t op_type int comment '操作类型' cascade;

describe waybill_route_link partition(dt='2021-01-31');

create table employee(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
row format delimited fields terminated by ',' lines terminated by '\n'
stored as textfile;

-- 当创建外部表时，不会去hbase创建对应的table，同理删除该hive表也不会删除hbase对应的table
-- 我们【通常】是使用外部表的形式来连接hbase表，因为hbase表一般都是已存在的，我们很少需要hive来管理hbase表。我们之所以使用hive来连接hbase，是因为我们需要对hbase表中的数据进行分析。
create external table hbase_emp(
    emp_no string,
    emp_name string,
    dept_name string,
    dept_no string,
    create_dt string,
    salary int,
    ts bigint
)
-- 使用hbase的InputFormat和OutputFormat
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
-- 为HBaseStorageHandler配置属性
with serdeproperties (
    -- 使用hbase.columns.mapping配置hive表和hbase表的对应关系，赋值的顺序必须和hive表字段的顺序一致。其中【:key】代表hive表中对应的字段作为rowkey，【:timestamp】代表hive表中对应的字段作为timestamp
    -- 注意：相比于【:key】，【:timestamp】不是必须给出的，但是【:timestamp】给出后，当往该表插入数据时，就必须要给【:timestamp】对应的字段赋值
    -- 在查询时，【:timestamp】对应的字段返回的值是相同rowkey下的所有数据中最大的那个
    "hbase.columns.mapping" = ":key,info:name,dept:name,dept:no,secret:create_dt,secret:salary,:timestamp",
    -- 要映射的hbase的表名
    "hbase.table.name" = "emp"
);

select * from hbase_emp;

-- 通过将hbase集成到hive，就可以对hbase表中数据进行分析了
-- hbase是存储型数据库，在他里面没有给任何用于数据计算的函数（例如sum、avg等），因此hbase没有数据分析能力。只能集成到hive，依托hive的数据分析能力来对hbase表中的数据进行分析
select *
from (
    select dept_name,words,count(*) cnt
    from hbase_emp lateral view explode(split(emp_name,'')) my_table as words where words<>''
    group by dept_name,words
    having count(*) > 3
) t
distribute by dept_name sort by dept_name,cnt;

-- 创建一个管理表，创建后会去hbase创建对应的表，在删除该hive表时也会删除对应hbase的表
-- 我们知道管理表管理两部分数据：一部分是metastore中的元数据，另一部分是实际数据，在这里就是管理hbase的table和table中的数据
-- 我们【很少】使用管理表来与hbase集成的hive表，因为将hbase集成到hive并不是为了让hive来管理hbase中的数据，而是让hive来分析hbase中的数据
create table hbase_emp_enrich(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    create_dt string,
    salary bigint,
    dept_avg_salary bigint,
    salary_compare int,
    ts bigint
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
    "hbase.table.name"="emp_enrich",
    "hbase.columns.mapping"=":key,info:name,dept:no,dept:name,info:create_dt,secret:salary,dept:avg_salary,secret:salary_compare,:timestamp"
);

insert into hbase_emp_enrich
select
    emp_no,
    emp_name,
    dept_no,
    dept_name,
    create_dt,
    salary,
    dept_avg_salary,
    case when salary>dept_avg_salary then '1' else '0' end salary_compare,
    ts
from (
    select
        *,
        avg(salary) over(partition by dept_no) dept_avg_salary
    from hbase_emp
) t;

insert into hbase_emp_enrich
select
    t.emp_no,
    t.emp_name,
    t.dept_no,
    t.dept_name,
    t.create_dt,
    t.salary,
    d.dept_avg_salary,
    case when salary>dept_avg_salary then '1' else '0' end salary_compare,
    ts
from hbase_emp t
         left join (
    select dept_no,
           avg(salary) dept_avg_salary
    from hbase_emp
    group by dept_no
) d
on t.dept_no = d.dept_no;

-- 和hbase关联的table无法被truncate，报错：SemanticException [Error 10147]: Cannot truncate non-native table hbase_emp_enrich.
-- truncate table hbase_emp_enrich;

create external table hbase_dept(
    dept_no string,
    dept_name string,
    dept_level string,
    tel string,
    level string,
    salary int,
    ts bigint
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
    "hbase.table.name"="dept",
    "hbase.columns.mapping"=":key,info:name,info:level,info:tel,tmp:level,tmp:salary,:timestamp"
);

-- 当向与hbase集成的table写入数据时，如果table配置了:timestamp对应的字段(在这里是ts字段)，那么必须给该字段赋值
insert into hbase_dept values('1000','hello world','999999','186','L0',3000,10);
-- 在查询一个rowkey下的所有字段数据时，由于在hbase中每一个字段是一条KV数据，都会有对应的时间戳，因此在hive表中ts字段返回的是该rowkey下所有KV数据的timestamp最大的那个
select * from hbase_dept;

insert into hbase_dept
select
    dept_no,
    dept_name,
    dept_level,
    null,
    concat('L',dept_level) level,
    round(avg_salary) salary,
    unix_timestamp() * 1000 ts
from(
    select
        *,
        floor(avg_salary/1000) dept_level
    from(
        select
            dept_no,
            dept_name,
            avg(salary) avg_salary
        from hbase_emp
        group by dept_no,dept_name
    ) a
) t;

create table employee(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
row format delimited fields terminated by ',' lines terminated by '\n'
stored as textfile;

load data inpath '/data/employee' overwrite into table employee;

create table hbase_employee(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
    "hbase.table.name"="employee",
    "hbase.columns.mapping"=":key,info:name,dept:no,dept:name,info:sex,info:create_dt,secret:salary"
);

insert into hbase_employee select * from employee;

insert into hbase_dept
select
    dept_no,
    dept_name,
    dept_level,
    null,
    concat('L',dept_level) level,
    round(avg_salary) salary,
    unix_timestamp() * 1000 ts
from(
    select
        *,
        floor(avg_salary/1000) dept_level
    from(
            select
                dept_no,
                dept_name,
                avg(salary) avg_salary
            from hbase_employee
            group by dept_no,dept_name
        ) a
) t;

create external table hbase_student(
    rowkey string,
    name string,
    age string,
    sex string,
    class string,
    ts bigint
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
    "hbase.table.name"="student",
    "hbase.columns.mapping"=":key,info:name,info:age,info:sex,info:class,:timestamp"
);

select * from hbase_student;
insert into hbase_student(rowkey,sex,age,ts) values('1001','male','17',12)