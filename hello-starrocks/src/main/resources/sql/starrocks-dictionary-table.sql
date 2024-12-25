CREATE TABLE mydb.`dim_data_conf` (
    `dimension` int(11) NOT NULL COMMENT "维度。1:省区",
    `data_code` varchar(100) NOT NULL COMMENT "数据编码",
    `data_name` varchar(500) NOT NULL COMMENT "数据名称",
    `yn` int(11) NULL COMMENT "有效标志",
    `ts` bigint(20) NOT NULL COMMENT "时间戳"
)
PRIMARY KEY(`dimension`, `data_code`)
COMMENT "维度基础配置信息"
DISTRIBUTED BY HASH(`dimension`) BUCKETS 1
PROPERTIES (
    "replication_num" = "3",
    "in_memory" = "false",
    "storage_format" = "DEFAULT",
    "enable_persistent_index" = "false",
    "compression" = "LZ4"
);

select dict_mapping('mydb.dim_data_conf',1,'100000','data_name',true);

select dict_mapping('mydb.dim_data_conf',1,province_code,'data_name') from mydb.realtime_delivery_invocation_test_index where province_code is not null and province_code <> '';

# 创建一个全局字典表，用于将source表中的varchar类型的erp字段映射成int类型的字段，进而在后续使用bitmap进行精准去重。
# 全局字典表的要求：
# 1.必须是主键表，以字典的key作为主键
# 2.需要有自增列，作为字典的value
create table mydb.my_dict_tbl(
    erp varchar(200),
    erp_id bigint auto_increment
)
primary key (erp)
distributed by hash(erp) buckets 1;

# 使用从source表的数据，写入到字典表。利用auto_increment特性来生成varchar类型的erp字段到int类型的字段的映射值。
insert into mydb.my_dict_tbl(erp) select distinct erp from realtime_delivery_invocation_test_index;

# 使用全局字典的方式一：
# 在查询source表时，通过使用dict_mapping函数，从字典表中获取指定字典key的value数据
select erp,dict_mapping('mydb.my_dict_tbl',erp,'erp_id') erp_id from mydb.realtime_delivery_invocation_test_index;

# 使用全局字典的方式二：
# 新建一个表，在表中增加一个计算字段，该字段使用dict_mapping函数以及字典key的字段，来生成计算字段的值。也就是下面的erp_id字段。
create table mydb.realtime_delivery_invocation_test_index_with_erp_id(
    `dt` date NULL COMMENT "",
    `apiName` varchar(500) NULL COMMENT "",
    `invoke_tm` datetime NULL COMMENT "",
    `apiGroupName` varchar(500) NULL COMMENT "",
    `appId` varchar(500) NULL COMMENT "",
    `erp` varchar(500) NULL COMMENT "",
    `endDate` varchar(500) NULL COMMENT "",
    `theaterCode` varchar(500) NULL COMMENT "",
    `waybillSource` varchar(500) NULL COMMENT "",
    `deliveryType` varchar(500) NULL COMMENT "",
    `siteName` varchar(500) NULL COMMENT "",
    `deliveryThirdType` varchar(500) NULL COMMENT "",
    `udataLimit` varchar(500) NULL COMMENT "",
    `province_code` varchar(500) NULL COMMENT "",
    `isExpress` varchar(500) NULL COMMENT "",
    `productSubType` varchar(500) NULL COMMENT "",
    `goodsType` varchar(500) NULL COMMENT "",
    `isKa` varchar(500) NULL COMMENT "",
    `areaCode` varchar(500) NULL COMMENT "",
    `orgCode` varchar(500) NULL COMMENT "",
    `partitionCode` varchar(500) NULL COMMENT "",
    `deliverySubType` varchar(500) NULL COMMENT "",
    `rejectionRoleId` varchar(500) NULL COMMENT "",
    `isZy` varchar(500) NULL COMMENT "",
    `productType` varchar(500) NULL COMMENT "",
    `siteDimension` varchar(500) NULL COMMENT "",
    `waybillDimension` varchar(500) NULL COMMENT "",
    `rdm` int(11) NULL COMMENT "",
    erp_id bigint as dict_mapping('mydb.my_dict_tbl',erp),
    INDEX rdm_index (`rdm`) USING BITMAP COMMENT '',
    INDEX siteDimension_index (`siteDimension`) USING BITMAP COMMENT ''
)
DUPLICATE KEY(`dt`, `apiName`)
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES (
    "bloom_filter_columns" = "erp",
    "bucket_size" = "4294967296",
    "compression" = "LZ4",
    "fast_schema_evolution" = "true",
    "replicated_storage" = "true",
    "replication_num" = "3"
);
# 从source表中读取数据，写入到dest表。借助计算字段以及dict_mapping函数，在数据导入时会自动：
# 1.使用当前数据erp字段的值来查询全局字典表
# 2.将从字典表查询的结果写入到计算字段erp_id中
insert into mydb.realtime_delivery_invocation_test_index_with_erp_id select * from mydb.realtime_delivery_invocation_test_index;
# 查询dest表，发现自动生成了erp字段映射的int类型字段erp_id的值，该值是从全局字典表中获取到的。后续可以使用erp_id字段来进行对erp的精准去重统计。
# +-------------------------+-----------+------+
# |apiName                  |erp        |erp_id|
# +-------------------------+-----------+------+
# |deliveryMonitorGatherBpDd|liangdezhi3|303   |
# |deliveryMonitorGatherBpDd|liangdezhi3|303   |
# |deliveryMonitorGatherBpDd|liangdezhi3|303   |
# |deliveryMonitorGatherBpDd|zhuxiangli3|7     |
# |deliveryMonitorGatherBpDd|huanganle1 |137   |
# |deliveryMonitorGatherBpDd|zhuxiangli3|7     |
# +-------------------------+-----------+------+
select apiName,erp,erp_id from mydb.realtime_delivery_invocation_test_index_with_erp_id;