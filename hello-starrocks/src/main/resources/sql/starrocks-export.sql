use mydb;

show tables;

# 使用export命令导出本地表到文件系统
# 如果TO "hdfs://namenode:9000/upload/realtime_delivery_invocation/"，则是把所有导出的文件写入到upload/realtime_delivery_invocation目录下
# 如果TO "hdfs://namenode:9000/upload/realtime_delivery_invocation/log_"，导出的文件也是在upload/realtime_delivery_invocation目录下，但是所有导出的文件都是以【log_】作为前缀
# 导出的文件名举例：/upload/realtime_delivery_invocation_1/log_ffe8ecba-9ffa-11ef-8ccd-0242c0a83006_0_0.csv，其中ffe8ecba-9ffa-11ef-8ccd-0242c0a83006是queryId
# 参数【include_query_id】用于指定导出文件名中是否包含queryId（默认值：true）
# 参数【load_mem_limit】用于指定单个BE节点上的内存使用上限（单位：byte，默认值：2147483648(即2GB)）
# 参数【timeout】用于指定导出任务的超时时间（单位：秒。默认值：86400（1 天））。
EXPORT TABLE realtime_delivery_invocation
TO "hdfs://namenode:9000/upload/realtime_delivery_invocation/log_"
PROPERTIES (
    "column_separator" = ",",
    "line_delimiter" = "\n",
    "load_mem_limit" = "2147483648",
    "timeout" = "3600",
    "include_query_id" = "true"
)
WITH BROKER;

# 按创建时间倒序查询导出任务
# +-----+------------------------------------+--------+--------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------+-------------------+-------------------+-------------------+-------+--------+
# |JobId|QueryId                             |State   |Progress|TaskInfo                                                                                                                                                                                           |Path                                                       |CreateTime         |StartTime          |FinishTime         |Timeout|ErrorMsg|
# +-----+------------------------------------+--------+--------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------+-------------------+-------------------+-------------------+-------+--------+
# |12907|ffe8ecba-9ffa-11ef-8ccd-0242c0a83006|FINISHED|100%    |{"partitions":["*"],"column separator":"\t","columns":["*"],"tablet num":37,"broker":"","coord num":1,"db":"mydb","tbl":"realtime_delivery_invocation","row delimiter":"\n","mem limit":2147483648}|hdfs://namenode:9000/upload/realtime_delivery_invocation_1/|2024-11-11 15:03:05|2024-11-11 15:03:06|2024-11-11 15:03:07|3600   |null    |
# |12899|def9a41f-9ff9-11ef-8ccd-0242c0a83006|FINISHED|100%    |{"partitions":["*"],"column separator":",","columns":["*"],"tablet num":37,"broker":"","coord num":1,"db":"mydb","tbl":"realtime_delivery_invocation","row delimiter":"\n","mem limit":2147483648} |hdfs://namenode:9000/upload/realtime_delivery_invocation/  |2024-11-11 14:55:00|2024-11-11 14:55:01|2024-11-11 14:55:02|3600   |null    |
# |12883|daee7120-9ff8-11ef-8ccd-0242c0a83006|FINISHED|100%    |{"partitions":["*"],"column separator":",","columns":["*"],"tablet num":37,"broker":"","coord num":1,"db":"mydb","tbl":"realtime_delivery_invocation","row delimiter":"\n","mem limit":2147483648} |hdfs://namenode:9000/upload/                               |2024-11-11 14:47:44|2024-11-11 14:47:46|2024-11-11 14:48:03|3600   |null    |
# +-----+------------------------------------+--------+--------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------+-------------------+-------------------+-------------------+-------+--------+
show export order by CreateTime desc;
# 按照queryId查询导出任务
show export where queryId="daee7120-9ff8-11ef-8ccd-0242c0a83006";