# 【总结】
# 1.如果没有启动资源组、查询队列
# 当短时间到来大量查询时，FE会无条件地处理每一个请求，将查询任务分配给每一个BE，直到这些任务执行完毕。
# 2.当启动了资源组
# 资源组可以【阻断新查询】。当短时间内到来大量查询时，FE会一个一个地处理查询任务。当FE发现正在执行的查询数量超过资源组的concurrency_limit配置后，则会拒绝执行后续的查询任务，避免同时处理大量任务而拖垮集群。
# 资源组还可以【熔断大查询】。当正在执行的任务的scan量或资源使用量超过阈值，FE会停止该任务，避免大查询一直执行，拖垮集群。
# 3.当启动了查询队列
# 查询队列可以【暂缓新查询】。当短时间内到来大量查询时，FE会一个一个地处理查询任务。直到FE发现单个BE的资源使用超过了查询队列配置的资源阈值，则将后面的查询放到查询队列中。当BE的资源使用情况降低至队列阈值之下时，才会从查询队列中拿出任务并执行。
# 查询队列也可以【阻断新查询】。当查询队列满了以后，FE会阻断新进入的查询。当查询队列中的任务长时间在队列中时，FE也会阻断该查询。

# 启动资源组后，当发起一个查询时：
# 1.FE先找到当前查询匹配的资源组
# 2.FE根据BE汇报的资源使用情况以及资源组的阈值，判断是否可以执行该任务
# 3.如果可以执行，则分派给BE进行执行，并将执行结果返回给客户端
# 4.如果不可以，则直接给客户端响应错误
# 启动查询队列后，当发起一个查询时：
# 1.FE根据BE汇报的资源使用情况以及查询队列或资源组的阈值，判断是否可以执行该任务
# 2.如果可以执行，则将查询转换为执行计划，分发给各个BE
# 3.如果可以执行，则分派给BE进行执行，并将执行结果返回给客户端
# 4.当集群资源使用下降到查询队列的阈值之下时，FE从队列中拿取出该查询任务并执行
# 5.执行完毕后，将执行结果返回给客户端

# 我们看这个资源组，当匹配到这个资源组的并发查询数量大于concurrency_limit，那么新提交的请求会被阻断查询。报错：Exceed concurrency limit: 10 backend [id=10002] [host=starrocks-be-0]。
# 阻断查询可以很好地保护sr集群不被过度消耗，避免查询高峰到来后压垮集群。这种在大促期间尤为重要，哪怕集群响应慢点，也不能让集群挂了，无法对外提供服务。
# 但阻断查询对集群使用方不太友好，因为提交的查询请求被集群直接驳回了。一种更好的方式是，当集群【当前负载】比较高时，将新来的查询放到一个查询队列中，暂不执行。当集群【当前负载】降低了，再从队列中拿出查询任务，执行该查询。
CREATE RESOURCE GROUP bigQuery
TO
    (db='mydb')
WITH (
    'cpu_weight' = '2',
    'mem_limit' = '20%',
    'big_query_cpu_second_limit' = '100',
    'big_query_scan_rows_limit' = '1000',
    'big_query_mem_limit' = '1073741824',
    'concurrency_limit' = '10'
);

# 【启动查询队列】
# 查询队列默认是不启动的，需要手动启动。
# 为导入任务启用查询队列
SET GLOBAL enable_query_queue_load = true;
# 为 SELECT 查询启用查询队列
SET GLOBAL enable_query_queue_select = true;
# 为统计信息查询启用查询队列
SET GLOBAL enable_query_queue_statistic = true;

# 【配置资源阈值】
# 上面说了，查询队列功能是在集群【当前负载】较高时，才会将新的查询放入到查询队列中。那么就需要有一些阈值参数来指出怎样算作集群负载高。
# 单个 BE 节点中并发查询上限。仅在设置为大于 0 后生效。设置为 0 表示没有限制。
# 当【单个BE】上的并行执行的请求超过该配置后，新的查询请求就会被放到查询队列中。
SET GLOBAL query_queue_concurrency_limit = 100;
# 单个 BE 节点中内存使用百分比上限。仅在设置为大于 0 后生效。设置为 0 表示没有限制。取值范围：[0, 1]
# 当【单个BE】内存使用的百分比超过该配置后，新的查询请求就会被放到查询队列中。
SET GLOBAL query_queue_mem_used_pct_limit = 0.4;
# 单个 BE 节点中 CPU 使用千分比上限（即 CPU 使用率 * 1000）。仅在设置为大于 0 后生效。设置为 0 表示没有限制。取值范围：[0, 1000]
# 当【单个BE】cpu使用的千分比超过该配置后，新的查询请求就会被放到查询队列中。
SET GLOBAL query_queue_cpu_used_permille_limit = 500;

# 【资源组粒度的阈值配置】
# 如果短时间到来的查询并发量大于匹配的资源组的concurrency_limit配置，但小于全局的query_queue_concurrency_limit配置，那么查询请求依然会被熔断，报以下错误。
# Exceed concurrency limit: 5 backend [id=10003] [host=starrocks-be-2]
# 这是因为全局阈值配置只有一个，当前BE资源使用情况超过了资源组的配置，而没有超过全局队列的配置，是按照资源组的处理方式进行阻断。我们期望的是资源使用超过匹配的资源组的【concurrency_limit】或【max_cpu_cores】阈值时，也可以进入到查询队列，而非直接阻断。
# 此时我们可以通过以下配置启用资源组粒度查询队列。
SET GLOBAL enable_group_level_query_queue = true;
# concurrency_limit：该资源组在单个BE节点中并发查询上限。仅在设置为大于 0 后生效。
# max_cpu_cores：该资源组在单个 BE 节点中使用的 CPU 核数上限。仅在设置为大于 0 后生效。取值范围：[0, avg_be_cpu_cores]，其中 avg_be_cpu_cores 表示所有 BE 的 CPU 核数的平均值。
ALTER RESOURCE GROUP bigQuery WITH (
    'concurrency_limit' = '5'
);

# 【FE如何获取集群中BE的资源使用情况】
# 针对BE资源使用情况：BE每隔一秒向 FE 报告资源使用情况，当查询请求提交到FE后，FE会根据集群中BE的资源使用情况来决定执行查询还是将查询放到查询队列。
# 针对BE执行的查询数量：所有 FE 正在运行的查询数量 num_running_queries 由 Leader FE 集中管理。每个 Follower FE 在发起和结束一个查询时，会通知 Leader FE，从而可以应对短时间内查询激增超过了 concurrency_limit 的场景。
# 【谁来判断查询任务要不要进入到查询队列】
# FE来判断。通过BE的资源汇报情况，FE可以知道每个BE的资源使用情况，进而判断新的查询是否要进入到队列中。
# 【何时将查询任务放到资源队列】
# 发起一个查询时，如果【FE】根据【BE】的资源汇报情况，发现【任意BE】的【任意一项资源】占用超过了全局粒度或资源组粒度的资源阈值，那么【FE】会将查询放到查询队列中
# 【何时从查询队列中拿取查询任务并执行】
# 直到【所有BE】的【所有资源】都没有超过阈值，【FE】才会从查询队列中拿取任务并执行。

# 【配置查询队列】
# 当查询队列中放入了很多查询任务，我们就需要考虑两个问题：
# 1.查询队列的长度。不能无限制地往队列里放，那样队列中的需求永远也清不完了。
# 2.查询队列中任务的最大等待时间。当一个任务放到查询队列后，假设队列中任务消解地很慢，那么这个任务就长时间在队列里。为了清理队列，不能无限制地让这个任务一直在队列中，当该任务在队列中一段时间后，从队列中剔除该任务，并阻止该任务进行查询。
# 可以看到，查询队列是为熔断查询做【兜底】。启动查询队列后，从资源组直接熔断查询变为将查询放入查询队列，使查询晚一点得到响应，不至于直接报错。但查询队列的兜底也有些底线，当队列中【任务过多】或【任务长时间在队列】中，依然会【熔断】该任务。

# 队列中查询数量的上限。当达到此阈值时，新增查询将被拒绝执行。仅在设置为大于 0 后生效。默认值：1024。
# 当短时间内来了一大批查询，超过了query_queue_concurrency_limit的配置，导致大量查询任务被添加到查询队列中。而当查询队列中的任务超过query_queue_max_queued_queries配置，就会报以下错误。
# Failed to allocate resource to query: com.starrocks.common.UserException: Resource is not enough and the number of pending queries exceeds capacity [100], you could modify the session variable [query_queue_max_queued_queries] to make more query can be queued
set GLOBAL query_queue_max_queued_queries = 1024;
# 队列中单个查询的最大超时时间。当达到此阈值时，该查询将被拒绝执行。单位：秒。默认值：300。
# 当查询队列中的一个任务长时间没有被执行，就会从队列中剔除该任务，并阻止该任务的执行。
# Failed to allocate resource to query: pending timeout [2], you could modify the session variable [query_queue_pending_timeout_second] to pending more time
set GLOBAL query_queue_pending_timeout_second = 10;

# 【观察查询队列】
# 观察BE的资源使用情况
# 因为是否将查询任务放到查询队列都是和【单个BE】的查询任务数量、cpu与内存使用量有关，因此通过show proc '/backends'可以看到当前每个BE的资源使用情况。
# +---------+--------------+-------------+------+--------+--------+-------------------+-------------------+-----+--------------------+---------------------+---------+----------------+-------------+-------------+-------+--------------+------+-------------+------------------------------------------------------+-----------------+-----------+--------+--------+-----------------+----------+----------+-------------------------------------------------+--------+
# |BackendId|IP            |HeartbeatPort|BePort|HttpPort|BrpcPort|LastStartTime      |LastHeartbeat      |Alive|SystemDecommissioned|ClusterDecommissioned|TabletNum|DataUsedCapacity|AvailCapacity|TotalCapacity|UsedPct|MaxDiskUsedPct|ErrMsg|Version      |Status                                                |DataTotalCapacity|DataUsedPct|CpuCores|MemLimit|NumRunningQueries|MemUsedPct|CpuUsedPct|DataCacheMetrics                                 |Location|
# +---------+--------------+-------------+------+--------+--------+-------------------+-------------------+-----+--------------------+---------------------+---------+----------------+-------------+-------------+-------+--------------+------+-------------+------------------------------------------------------+-----------------+-----------+--------+--------+-----------------+----------+----------+-------------------------------------------------+--------+
# |10002    |starrocks-be-0|9050         |9060  |8040    |8060    |2025-01-06 11:10:44|2025-01-09 18:45:26|true |false               |false                |713      |221.787 MB      |89.644 GB    |465.627 GB   |80.75 %|80.75 %       |      |3.3.5-6d81f75|{"lastSuccessReportTabletsTime":"2025-01-09 18:44:55"}|89.861 GB        |0.24 %     |4       |5.499GB |5                |8.85 %    |19.2 %    |Status: Normal, DiskUsage: 0B/0B, MemUsage: 0B/0B|        |
# |10001    |starrocks-be-1|9050         |9060  |8040    |8060    |2025-01-06 11:10:44|2025-01-09 18:45:26|true |false               |false                |714      |221.787 MB      |90.802 GB    |465.627 GB   |80.50 %|80.50 %       |      |3.3.5-6d81f75|{"lastSuccessReportTabletsTime":"2025-01-09 18:45:08"}|91.018 GB        |0.24 %     |4       |5.499GB |6                |9.22 %    |19.7 %    |Status: Normal, DiskUsage: 0B/0B, MemUsage: 0B/0B|        |
# |10003    |starrocks-be-2|9050         |9060  |8040    |8060    |2025-01-06 11:10:44|2025-01-09 18:45:26|true |false               |false                |714      |221.753 MB      |89.623 GB    |465.627 GB   |80.75 %|80.75 %       |      |3.3.5-6d81f75|{"lastSuccessReportTabletsTime":"2025-01-09 18:44:37"}|89.839 GB        |0.24 %     |4       |5.499GB |8                |9.32 %    |16.6 %    |Status: Normal, DiskUsage: 0B/0B, MemUsage: 0B/0B|        |
# +---------+--------------+-------------+------+--------+--------+-------------------+-------------------+-----+--------------------+---------------------+---------+----------------+-------------+-------------+-------+--------------+------+-------------+------------------------------------------------------+-----------------+-----------+--------+--------+-----------------+----------+----------+-------------------------------------------------+--------+
show proc '/backends';

# SHOW PROCESSLIST
# 可以通过SHOW PROCESSLIST查询到所有【正在运行】或【在队列中等待】的任务，IsPending=true则为在队列中等待的任务。
# +----+----+------------------+--+-------+-------------------+----+-----+---------------------------------------------------------------+---------+
# |Id  |User|Host              |Db|Command|ConnectionStartTime|Time|State|Info                                                           |IsPending|
# +----+----+------------------+--+-------+-------------------+----+-----+---------------------------------------------------------------+---------+
# |8812|root|192.168.48.1:56258|  |Sleep  |2025-01-09 21:11:07|0   |OK   |select * from mydb.hello_world limit 10                        |false    |
# |8813|root|192.168.48.1:56264|  |Query  |2025-01-09 21:11:07|0   |OK   |select * from mydb.hello_world limit 10                        |true     |
# |8814|root|192.168.48.1:56286|  |Query  |2025-01-09 21:11:07|0   |OK   |select * from mydb.hello_world limit 10                        |true     |
# |8815|root|192.168.48.1:56294|  |Query  |2025-01-09 21:11:07|0   |ERR  |select * from mydb.hello_world limit 10                        |false    |
# +----+----+------------------+--+-------+-------------------+----+-----+---------------------------------------------------------------+---------+
SHOW PROCESSLIST;

# SHOW RUNNING QUERIES
# 也可以通过SHOW RUNNING QUERIES来查询所有【正在运行】或【在队列中等待】的任务。与SHOW PROCESSLIST不同的是，该命令可以查询到更详细的信息。比如任务何时触发的（StartTime），在队列中的超时时间（PendingTimeout=StartTime+query_queue_pending_timeout_second），查询任务的超时时间（QueryTimeout=StartTime+query_timeout）。
# +------------------------------------+---------------+-------------------+-------------------+-------------------+--------+-----+---------+---+-------------------------------+-------------------+
# |QueryId                             |ResourceGroupId|StartTime          |PendingTimeout     |QueryTimeout       |State   |Slots|Fragments|DOP|Frontend                       |FeStartTime        |
# +------------------------------------+---------------+-------------------+-------------------+-------------------+--------+-----+---------+---+-------------------------------+-------------------+
# |bdfee5bd-cef6-11ef-9aec-0242c0a83006|46255          |2025-01-10 10:01:02|2025-01-10 10:01:12|2025-01-10 10:06:02|PENDING |1    |2        |0  |e08012839c6d_9010_1730946513652|2025-01-06 11:10:25|
# |be6dfaad-cef6-11ef-9aec-0242c0a83006|46255          |2025-01-10 10:01:02|2025-01-10 10:01:12|2025-01-10 10:06:02|FINISHED|1    |2        |0  |e08012839c6d_9010_1730946513652|2025-01-06 11:10:25|
# |bdfac692-cef6-11ef-9aec-0242c0a83006|46255          |2025-01-10 10:01:02|2025-01-10 10:01:12|2025-01-10 10:06:02|FINISHED|1    |2        |0  |e08012839c6d_9010_1730946513652|2025-01-06 11:10:25|
# |be715622-cef6-11ef-9aec-0242c0a83006|46255          |2025-01-10 10:01:02|2025-01-10 10:01:12|2025-01-10 10:06:02|RUNNING |1    |2        |0  |e08012839c6d_9010_1730946513652|2025-01-06 11:10:25|
# |bdfb14c1-cef6-11ef-9aec-0242c0a83006|46255          |2025-01-10 10:01:02|2025-01-10 10:01:12|2025-01-10 10:06:02|RUNNING |1    |2        |0  |e08012839c6d_9010_1730946513652|2025-01-06 11:10:25|
# |bdfc4d6a-cef6-11ef-9aec-0242c0a83006|46255          |2025-01-10 10:01:02|2025-01-10 10:01:12|2025-01-10 10:06:02|RUNNING |1    |2        |0  |e08012839c6d_9010_1730946513652|2025-01-06 11:10:25|
# |be7351fd-cef6-11ef-9aec-0242c0a83006|46255          |2025-01-10 10:01:02|2025-01-10 10:01:12|2025-01-10 10:06:02|PENDING |1    |2        |0  |e08012839c6d_9010_1730946513652|2025-01-06 11:10:25|
# |be6a2af3-cef6-11ef-9aec-0242c0a83006|46255          |2025-01-10 10:01:02|2025-01-10 10:01:12|2025-01-10 10:06:02|PENDING |1    |2        |0  |e08012839c6d_9010_1730946513652|2025-01-06 11:10:25|
# +------------------------------------+---------------+-------------------+-------------------+-------------------+--------+-----+---------+---+-------------------------------+-------------------+
SHOW RUNNING QUERIES;

# 针对已完成的任务，可以从审计日志中查看该任务在队列中的等待时间。pendingTimeMs是任务在队列中的等待时间，queryTime是任务总共的查询时间（包含在队列中等待的时间）。用queryTime - pendingTimeMs则是任务真正的执行用时。
# +------------------------------------+-------------------+----------+------------------+----+--------------+-------------+---------------+--+-----+---------+---------+---------+--------+----------+---------+------------+------+-------+------------+---------------------------------------+--------------------------------+------------+------------+-------------+------------+------+
# |queryId                             |timestamp          |queryType |clientIp          |user|authorizedUser|resourceGroup|catalog        |db|state|errorCode|queryTime|scanBytes|scanRows|returnRows|cpuCostNs|memCostBytes|stmtId|isQuery|feIp        |stmt                                   |digest                          |planCpuCosts|planMemCosts|pendingTimeMs|candidateMVs|hitMvs|
# +------------------------------------+-------------------+----------+------------------+----+--------------+-------------+---------------+--+-----+---------+---------+---------+--------+----------+---------+------------+------+-------+------------+---------------------------------------+--------------------------------+------------+------------+-------------+------------+------+
# |bed5bdbf-cef6-11ef-9aec-0242c0a83006|2025-01-10 10:01:03|slow_query|192.168.48.1:58810|root|'root'@'%'    |bigQuery     |default_catalog|  |EOF  |         |5906     |521      |10      |10        |4911153  |276608      |37921 |1      |e08012839c6d|select * from mydb.hello_world limit 10|71a41257c2c42ebdc7603d43fb3f2156|660         |0           |5517         |null        |null  |
# |bed4fa6e-cef6-11ef-9aec-0242c0a83006|2025-01-10 10:01:03|query     |192.168.48.1:59008|root|'root'@'%'    |bigQuery     |default_catalog|  |EOF  |         |4752     |521      |10      |10        |738068   |357896      |37920 |1      |e08012839c6d|select * from mydb.hello_world limit 10|                                |660         |0           |4687         |null        |null  |
# |bed150e6-cef6-11ef-9aec-0242c0a83006|2025-01-10 10:01:03|query     |192.168.48.1:60110|root|'root'@'%'    |bigQuery     |default_catalog|  |EOF  |         |4939     |521      |10      |10        |608705   |276608      |37912 |1      |e08012839c6d|select * from mydb.hello_world limit 10|                                |660         |0           |4779         |null        |null  |
# |bef13515-cef6-11ef-9aec-0242c0a83006|2025-01-10 10:01:03|slow_query|192.168.48.1:58658|root|'root'@'%'    |bigQuery     |default_catalog|  |EOF  |         |8423     |521      |10      |10        |4606858  |276608      |37943 |1      |e08012839c6d|select * from mydb.hello_world limit 10|71a41257c2c42ebdc7603d43fb3f2156|660         |0           |7696         |null        |null  |
# |bed7b992-cef6-11ef-9aec-0242c0a83006|2025-01-10 10:01:03|slow_query|192.168.48.1:58668|root|'root'@'%'    |bigQuery     |default_catalog|  |EOF  |         |5045     |521      |10      |10        |678570   |357896      |37924 |1      |e08012839c6d|select * from mydb.hello_world limit 10|71a41257c2c42ebdc7603d43fb3f2156|660         |0           |4824         |null        |null  |
# |bee09337-cef6-11ef-9aec-0242c0a83006|2025-01-10 10:01:03|slow_query|192.168.48.1:59482|root|'root'@'%'    |bigQuery     |default_catalog|  |EOF  |         |5444     |521      |10      |10        |2821429  |276608      |37929 |1      |e08012839c6d|select * from mydb.hello_world limit 10|71a41257c2c42ebdc7603d43fb3f2156|660         |0           |5191         |null        |null  |
# +------------------------------------+-------------------+----------+------------------+----+--------------+-------------+---------------+--+-----+---------+---------+---------+--------+----------+---------+------------+------+-------+------------+---------------------------------------+--------------------------------+------------+------------+-------------+------------+------+
select * from starrocks_audit_db__.starrocks_audit_tbl__ where stmt like '%hello_world%' order by timestamp desc;