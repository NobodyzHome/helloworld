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
insert into waybill_route_link partition(dp='ACTIVE',dt='2022-04-11') values("JDI_A",20,"MM"),("JDC_A",40,"II"),("JDP_A",10,"ZZ"),("JDD_A",50,"UU");

#!/usr/bin/env python3
#===============================================================================
# 程序名: app_zw_del_arrival_sum_5min.py
# 项目名: 智网-离线指标历史数据
# 业务方: 张丽()
# 产  品: 高峰()
# 背  景: 。
# 目  标:
# 输  出: 历史每天每5分钟的到货单量。
# 说  明: 每日的数据封装到相应的dt分区。
# 参  数:
# 规  则: 封装每日的到货汇总数据到 dt分区
# 周  期: 日-5分钟
# 源  表:
#
# 临时表:
#
# 目标表: app.app_zw_del_arrival_sum_5min   --营业部应到货数据5分汇总表
#
# 版  本: v1.0    2020-10-16     闫大建                     初稿
#
#===============================================================================
import sys
import os
import time
from datetime import datetime,date,timedelta
from HiveTask import HiveTask


ht = HiveTask()

#定义获取日期列表的函数
def getDay(begin_date, end_date):
    date_list = []
    start_date = datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    while start_date <= end_date:
        date_str = start_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        start_date += timedelta(days=1)
    return date_list

#设置日期
now_bus_day_str = ht.oneday(0,' - ')
last26_bus_day_str = ht.oneday(-26,' - ')
last365_bus_day_str = ht.oneday(-365,' - ')
last2_bus_day_str = ht.oneday(-2,' - ')
last1_bus_day_str = ht.oneday(-1,' - ')
last30_bus_day_str = ht.oneday(-30,' - ')

#定义需要合并小文件的分区范围，在最后的ht.exec_sql的传入参数中使用
#多分区目录合并
partition_dir = []

for date in getDay(now_bus_day_str, now_bus_day_str):
    partition_dir.append('dt='+date)

db_app='app'
tab_name='app_zw_del_arrival_sum_5min'

#SQL逻辑体
sql1="""
--设置hive执行参数
set hive.default.fileformat=Orc;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.hadoop.supports.splittable.combineinputformat=true;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=512000000;
set mapred.min.split.size.per.rack=512000000;
set hive.merge.size.per.task =512000000;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles = true;
set hive.merge.smallfiles.avgsize=512000000;
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.auto.convert.join = true ;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;

INSERT overwrite TABLE """+db_app+"""."""+tab_name+""" partition (dt = '"""+now_bus_day_str+"""')
SELECT
	SUBSTR(T0.last_exam_tm, 1, 10) last_exam_dt, --到货日期
	concat(SUBSTR(T0.last_exam_tm, 12, 2), ':', lpad(floor(SUBSTR(T0.last_exam_tm, 15, 2) / 5) * 5, 2, '0')) AS data_min, --五分钟汇总粒度
	concat(SUBSTR(T0.last_exam_tm, 1, 13), ':', lpad(floor(SUBSTR(T0.last_exam_tm, 15, 2) / 5) * 5, 2, '0')) AS virtual_time, --虚拟时间
	T2.org_id, --区域ID
	T2.region_name, --区域名称
	T2.zhanqv_code, --战区ID
	T2.zhanqv_name, --战区名称
	T2.area_id, --片区ID
	T2.area_name, --片区名称
	T2.partition_id, --分区ID
	T2.partition_name, --分区名称
	T0.site_code, --站点ID
	T2.SITE_NAME, --站点名称
	case when t5.order_level_type in ('wy028-001-001','wy028-001-002')
	     then 1
		 when t5.order_level_type in ('wy028-002-001','wy028-002-002')
		 then 2
		 when t5.order_level_type = 'wy028-003-001'
		 then 3
	else 100 end way_bill_source, --运单来源：1自营 2商家 3商业 100其他
	CASE
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) IN('0', 'B', 'C', '5')
			AND SUBSTR(t1.waybill_sign, 40, 1) = '0'
		THEN 1 --特惠送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '1'
			AND SUBSTR(t1.waybill_sign, 116, 1) IN('0', '1', '2', '3')
		THEN 2 --特快送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '4'
		THEN 2 --特快送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '2'
			AND SUBSTR(t1.waybill_sign, 16, 1) IN('1', '2', '3', '7', '8')
		THEN 2 --特快送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '3'
		THEN 2 --特快送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '9'
		THEN 4 --生鲜特快
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = 'A'
		THEN 5 --生鲜特惠
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '1'
		THEN 6 --生鲜专送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '7'
			AND SUBSTR(t1.waybill_sign, 29, 1) = '8'
		THEN 3 --同城速配
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '8'
		THEN 3 --同城速配
		WHEN SUBSTR(t1.waybill_sign, 87, 1) = '2'
			AND SUBSTR(t1.waybill_sign, 1, 1) = '6'
		THEN 3 --同城速配
		ELSE 99
	END AS prd_type, --产品类型
	CASE
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '1'
			AND SUBSTR(t1.waybill_sign, 116, 1) = '1'
		THEN 21 --特快航空
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '1'
			AND SUBSTR(t1.waybill_sign, 116, 1) = '2'
		THEN 22 --特快即日
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '1'
			AND SUBSTR(t1.waybill_sign, 116, 1) = '3'
		THEN 23 --特快次晨
		ELSE 299
	END AS quick_type, --特快送类型
	CASE
		WHEN SUBSTR(t1.waybill_sign, 57, 1) = '2'
		THEN 1
		ELSE 0
	END AS is_ka, -- 是否ka 1 是 0 否
	t2.type_enum, --配送方式
	t2.subtype_enum,--配送子类型
	t3.staff_role, --人员性质
	CASE
		WHEN t4.collection_money > 0
			AND SUBSTR(t1.waybill_sign, 40, 1) = '0'
		THEN 1
		ELSE 0
	END is_cod, --是否COD 1:是 0:否
	CASE
		WHEN SUBSTR(t1.waybill_sign, 62, 1) = '8'
		THEN 1
		ELSE 0
	END AS is_zy, --是否众邮 1:是 0:否
	CASE
		WHEN SUBSTR(t1.waybill_sign, 10, 1) IN('2', '5', '6', '7', '8', '9')
			OR SUBSTR(t1.ord_flag, 2, 1) IN('3', '4', '5', '6', '7', '8', '9')
		THEN 1
		ELSE 2
	END AS goods_type, --货物类型 1:生鲜 2:普货
	count(T1.waybill_code) AS exam_num,
	sum(package_qtty) package_qtty
FROM
    (
	SELECT
	     NVL(TMP1.waybill_code,TMP2.WAYBILL_CODE) WAYBILL_CODE ,
		 NVL(TMP1.site_code,TMP2.site_code) site_code,
		 NVL(TMP1.last_exam_operr_id,TMP2.last_exam_operr_id) last_exam_operr_id,
		 NVL(TMP1.last_exam_tm,TMP2.last_exam_tm) last_exam_tm
	FROM
		(
		 select
			 waybill_code
			 ,operator_site_id site_code
			 ,operator_user_id last_exam_operr_id
			 ,create_time  last_exam_tm
	    FROM
            (SELECT
                waybill_code,
                operator_site_id,
                operator_user_id,
                create_time,
                row_number() over(partition by waybill_code order by create_time desc)	rn
            FROM
                fdm.fdm_bd_waybill_package_state
            WHERE
                SUBSTR(create_time, 1, 10) = '"""+now_bus_day_str+"""'
                AND state = -460
            ) a
        where a.rn=1

		) tmp1

		full join
        (select
			 waybill_code
			 ,operator_site_id site_code
			 ,operator_user_id last_exam_operr_id
			 ,create_time  last_exam_tm
	    FROM
            (SELECT
                waybill_code,
                operator_site_id,
                operator_user_id  ,
                create_time,
                row_number() over(partition by waybill_code order by create_time desc)	rn
            FROM
                fdm.fdm_bd_waybill_package_state
            WHERE
                SUBSTR(create_time, 1, 10) = '"""+now_bus_day_str+"""'
                AND state = 80
            ) a
        where a.rn=1
		) tmp2
	on tmp1.waybill_code=tmp2.waybill_code
	)
	T0
    JOIN
	(
		SELECT
			waybill_code,
			outer_ord_flag,
			waybill_sign,
			distribute_type,
			ord_flag,
			package_qtty
		FROM
			cdm.cdm_dis_waybill_process_basic_det

		WHERE
		    DP='ACTIVE'  --如果需要3天之前的历史数据，此条件需要去除
			AND NOT
			(
				split(waybill_sign, '0') [0] = '2'
				AND size(split(waybill_sign, '0')) = 1
				AND waybill_code = sale_ord_id
			) --剔除无效运单
			--AND last_create_site_id NOT IN('566358', '566360', '566372', '566374', '566377', '566396', '566399')
			--AND coalesce(shelves_tm,'') = '' --剔除自提上架的运单
	)
	T1
ON T0.waybill_code=T1.waybill_code
JOIN
	(
		SELECT
			site_code,
			site_name,
			type_enum,
			subtype_enum,
			org_id,
			zhanqv_code,
			area_id,
			partition_id,
			region_name,
			zhanqv_name,
			area_name,
			partition_name
		FROM
			dim.dim_dis_base_site
		WHERE type_enum<> 64
	)
	T2
ON
	t0.site_code = t2.site_code
LEFT JOIN
	(
		SELECT
			staff_no,
			max(staff_role)	staff_role
		FROM
			fdm.fdm_basic_ql_base_staff_chain
		WHERE
			DP = 'ACTIVE'
			and staff_role is not null
			AND yn = 1
		group by staff_no
	)
	t3d
ON
	t0.last_exam_operr_id = t3.staff_no
LEFT JOIN
	(
		SELECT
			delivery_id,
			collection_money
		FROM
			fdm.fdm_receive_orderinfo_chain
		WHERE
			dp = 'ACTIVE'
			AND SUBSTR(create_time, 1, 10) >= '"""+last30_bus_day_str+"""'
	)
	t4
ON
	t0.waybill_code = t4.delivery_id
LEFT JOIN
    (    SELECT
            waybill_code,
            order_level_type
        FROM cdm.cdm_dis_del_waybill_info
	    WHERE DP='ACTIVE' --历史3天以上回算需删除此条件
	    and ship_bill_type_cd<>'22'
    ) t5
ON
    t0.waybill_code=t5.waybill_code
group by
    SUBSTR(T0.last_exam_tm, 1, 10) ,
	concat(SUBSTR(T0.last_exam_tm, 12, 2), ':', lpad(floor(SUBSTR(T0.last_exam_tm, 15, 2) / 5) * 5, 2, '0')) ,
	concat(SUBSTR(T0.last_exam_tm, 1, 13), ':', lpad(floor(SUBSTR(T0.last_exam_tm, 15, 2) / 5) * 5, 2, '0')),
	T2.org_id,
	T2.region_name,
	T2.zhanqv_code,
	T2.zhanqv_name,
	T2.area_id,
	T2.area_name,
	T2.partition_id,
	T2.partition_name,
	T0.site_code,
	T2.SITE_NAME,
	case when t5.order_level_type in ('wy028-001-001','wy028-001-002')
	     then 1
		 when t5.order_level_type in ('wy028-002-001','wy028-002-002')
		 then 2
		 when t5.order_level_type = 'wy028-003-001'
		 then 3
	else 100 end ,
	CASE
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) IN('0', 'B', 'C', '5')
			AND SUBSTR(t1.waybill_sign, 40, 1) = '0'
		THEN 1 --特惠送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '1'
			AND SUBSTR(t1.waybill_sign, 116, 1) IN('0', '1', '2', '3')
		THEN 2 --特快送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '4'
		THEN 2 --特快送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '2'
			AND SUBSTR(t1.waybill_sign, 16, 1) IN('1', '2', '3', '7', '8')
		THEN 2 --特快送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '3'
		THEN 2 --特快送
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '9'
		THEN 4 --生鲜特快
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = 'A'
		THEN 5 --生鲜特惠
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '1'
		THEN 6 --生鲜特惠
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '7'
			AND SUBSTR(t1.waybill_sign, 29, 1) = '8'
		THEN 3 --同城速配
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '8'
		THEN 3 --同城速配
		WHEN SUBSTR(t1.waybill_sign, 87, 1) = '2'
			AND SUBSTR(t1.waybill_sign, 1, 1) = '6'
		THEN 3 --同城速配
		ELSE 99
	END,
	CASE
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '1'
			AND SUBSTR(t1.waybill_sign, 116, 1) = '1'
		THEN 21 --特快航空
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '1'
			AND SUBSTR(t1.waybill_sign, 116, 1) = '2'
		THEN 22 --特快即日
		WHEN SUBSTR(t1.waybill_sign, 55, 1) = '0'
			AND SUBSTR(t1.waybill_sign, 31, 1) = '1'
			AND SUBSTR(t1.waybill_sign, 116, 1) = '3'
		THEN 23 --特快次晨
		ELSE 299
	END ,
	CASE
		WHEN SUBSTR(t1.waybill_sign, 57, 1) = '2'
		THEN 1
		ELSE 0
	END ,
	t2.type_enum, --配送方式
	t2.subtype_enum,
	t3.staff_role, --人员性质
	CASE
		WHEN t4.collection_money > 0
			AND SUBSTR(t1.waybill_sign, 40, 1) = '0'
		THEN 1
		ELSE 0
	END ,
	CASE
		WHEN SUBSTR(t1.waybill_sign, 62, 1) = '8'
		THEN 1
		ELSE 0
	END ,
	CASE
		WHEN SUBSTR(t1.waybill_sign, 10, 1) IN('2', '5', '6', '7', '8', '9')
			OR SUBSTR(t1.ord_flag, 2, 1) IN('3', '4', '5', '6', '7', '8', '9')
		THEN 1
		ELSE 2
	END

    """
ht.exec_sql(schema_name = db_app, table_name = tab_name, sql = sql1,merge_flag = True, merge_type='mr',merge_part_dir = partition_dir)


select TIMESTAMPDIFF(DAY,to_date('2022-03-08 15:33:22.0','yyyy-MM-dd'),CURRENT_DATE);
