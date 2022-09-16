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
partitioned by (dt string)
row format DELIMITED fields terminated by '#' lines terminated by '\n'
stored as textfile;

insert into waybill_route_link partition(dt='2022-09-10') values('JDA',21,10013,'xian_site','beijing_sort,beijing_site,xian_sort,xian_site'),('JDB',21,101,'shanxi_site','xian_sort,xian_site,shanxi_site'),('JDC',15,1101,'hainan',null);

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