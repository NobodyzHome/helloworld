# 设置资源组的目的
# 1.规划不同查询或导入任务能够使用的BE的cpu、内存等资源，也就是规划BE的资源应该怎么使用。
# 2.当该资源组的查询被认定为大查询时（通过big_query系列的配置），及时熔断该查询，避免大查询一直执行，拖垮整个集群。
# 3.当资源组内已有一定数量的查询正在进行时，阻断新提交的查询。避免在集群负载过高的情况下，还持续地提交新的查询请求。

# 资源组的组成
# 1.资源组主要由【分类器】和【资源限制】两部分组成。
# 2.分类器是在TO(xxx)部分声明的，用于指出哪些查询可以匹配到该资源组。
# 3.资源限制是在WITH(xxx)部分声明的，用于给出该资源组的资源限额。当该资源组下的查询的资源使用量超过了配置，资源组会熔断该查询，避免大查询一直执行，拖垮整个集群。
# 目前已知的会熔断查询的配置：mem_limit、concurrency_limit、big_query_cpu_second_limit、big_query_scan_rows_limit、big_query_mem_limit

# 当发起一个查询后：
# 1.遍历每一个资源组，根据分类器对该查询匹配度进行打分。得分最高的资源组则为该查询使用的资源组。
# 2.如果发起的查询没有匹配到任何资源组，则使用默认资源组default_wg。
CREATE RESOURCE GROUP bigQuery
TO
    (db='mydb')
WITH (
    'cpu_weight' = '2',
    'mem_limit' = '20%',
    'big_query_cpu_second_limit' = '100',
    'big_query_scan_rows_limit' = '1000',
    'big_query_mem_limit' = '1073741824',
    'concurrency_limit' = '1'
);

# 1.测试【big_query_mem_limit】
# 将资源组的mem_limit设置的很小，同时将其他指标设置大些，避免由其他指标触发熔断。观察大查询执行时的情况。
ALTER RESOURCE GROUP bigQuery WITH (
    'mem_limit' = '2%',
    'big_query_mem_limit' = '1073741824',
    'big_query_cpu_second_limit' = '100',
    'big_query_scan_rows_limit' = '6000000'
);
# 当发起一个大查询后，当BE在执行时，发现内存使用超过资源组配置，就会发起查询熔断。停止该查询，并报以下错误：
# Memory of bigQuery exceed limit. try consume:29360128 Backend: starrocks-be-2, Used: 109443864, Limit: 106288489.
# Mem usage has exceed the limit of the resource group [bigQuery]. You can change the limit by modifying [mem_limit] of this group: BE:10003
select salary,count(distinct name) from mydb.hello_world_source group by salary;

# 2.测试【big_query_scan_rows_limit】
# 将资源组的big_query_scan_rows_limit设置较小，观察查询一个大表时的情况
ALTER RESOURCE GROUP bigQuery WITH (
    'big_query_scan_rows_limit' = '100000'
);
# 该表有19w数据，需要进行全表扫描。该sql执行时会报以下错误。注意：提交该sql时，sql会执行，当执行scan到数据量超过资源组的阈值后，则会停止该查询。
# exceed big query scan_rows limit: current is 176128 but limit is 100000: BE:10002
select age,count(distinct name) cnt from mydb.hello_world_source group by age;
# 通过前缀索引减少要读取的文件，使需要scan的数据量减少到阈值之下，就可以正常执行了
select age,count(distinct name) cnt from mydb.hello_world_source where sex=1 and name='0001d' group by age;

# 3.测试【big_query_cpu_second_limit】
# 将big_query_cpu_second_limit设置的很小，观察执行情况。注意：单位是秒。
ALTER RESOURCE GROUP bigQuery WITH (
    'big_query_cpu_second_limit' = '3'
);
# 查询执行后，会执行一段时间。当超过资源组要求的cpu使用时间后，会停止该查询，并报出以下异常。
# exceed big query cpu limit: current is 3012622618ns but limit is 3000000000ns: BE:10002
select salary,count(distinct name) from mydb.hello_world_source group by salary;

# 4.测试【big_query_mem_limit】
# 将资源组的big_query_mem_limit设置的很小，观察大查询执行时的情况。注意：单位是byte。
ALTER RESOURCE GROUP bigQuery WITH (
    'big_query_mem_limit' = '100000'
);
# 查询执行后，在使用的内存超过资源组的阈值后，会停止查询并报以下错误。
# Memory of Group=bigQuery, Query44655236-c8f8-11ef-824d-0242c0a83005 exceed limit. Pipeline Backend: starrocks-be-0, fragment: 44655236-c8f8-11ef-824d-0242c0a8300b Used: 312080, Limit: 100000.
# Mem usage has exceed the big query limit of the resource group [Group=bigQuery, Query44655236-c8f8-11ef-824d-0242c0a83005]. You can change the limit by modifying [big_query_mem_limit] of this group: BE:10002
select salary,count(distinct name) from mydb.hello_world_source group by salary;

# 5.测试【concurrency_limit】
# 将concurrency_limit设置的很小，并通过sleep函数持续占用查询。观察已提交一个查询后，再发起一次查询的执行情况。
ALTER RESOURCE GROUP bigQuery WITH (
    'concurrency_limit' = '1'
);
# 以下sql会占用查询10s，我们在另一个客户端执行该sql，然后在当前客户端提交该sql。发现sql根本没有执行，直接报以下错误，说明资源组已经有1个查询正在进行了，在他没执行完之前，新进来的查询都直接拒绝，都不给执行的机会。
# Exceed concurrency limit: 1 backend [id=10002] [host=starrocks-be-0]
select name,sleep(10) from mydb.dict_tbl;

# 【强制查询使用的资源组】
# 我们可以通过session变量，强制指定当前会话提交的查询所使用的资源组。
SET resource_group = 'default_mv_wg';

# 【删除指定的资源组】
DROP RESOURCE GROUP bigQuery;

# 【查询所有资源组】
# +-------------+-----+----------+-------------------+---------+--------------------------+-------------------------+-------------------+-----------------+-------------------------+----------------------------------+
# |name         |id   |cpu_weight|exclusive_cpu_cores|mem_limit|big_query_cpu_second_limit|big_query_scan_rows_limit|big_query_mem_limit|concurrency_limit|spill_mem_limit_threshold|classifiers                       |
# +-------------+-----+----------+-------------------+---------+--------------------------+-------------------------+-------------------+-----------------+-------------------------+----------------------------------+
# |bigQuery     |44041|2         |0                  |20.0%    |3                         |6000000                  |1073741824         |null             |100%                     |(id=44042, weight=10.0, db='mydb')|
# |default_mv_wg|3    |1         |0                  |80.0%    |0                         |0                        |0                  |null             |80%                      |(id=0, weight=0.0)                |
# |default_wg   |2    |4         |0                  |100.0%   |0                         |0                        |0                  |null             |100%                     |(id=0, weight=0.0)                |
# +-------------+-----+----------+-------------------+---------+--------------------------+-------------------------+-------------------+-----------------+-------------------------+----------------------------------+
SHOW RESOURCE GROUPS ALL;

# 查询目前有任务执行的资源组的资源使用情况。如果资源组下没有正在执行的任务，则不显示该资源组。
# +--------+-----+--------------+---------------+---------------+----------------+
# |Name    |Id   |Backend       |BEInUseCpuCores|BEInUseMemBytes|BERunningQueries|
# +--------+-----+--------------+---------------+---------------+----------------+
# |bigQuery|44041|starrocks-be-0|0.0            |161392         |1               |
# |bigQuery|44041|starrocks-be-2|0.0            |114048         |1               |
# +--------+-----+--------------+---------------+---------------+----------------+
SHOW USAGE RESOURCE GROUPS;

# 【观察查询命中的资源组】
# 1.针对未被查询的sql，我们可以通过EXPLAIN VERBOSE sql，从【RESOURCE GROUP】字段中获取该查询将会使用的资源组
# RESOURCE GROUP: bigQuery
# PLAN COST
#   CPU: 1.3332959999999997E8
#   Memory: 5.180015399999999E7
EXPLAIN VERBOSE select salary,count(distinct name) from mydb.hello_world_source group by salary;

# 2.针对正在执行的查询，可以使用SHOW PROC '/current_queries'，从返回的【ResourceGroup】字段中获取该查询使用的资源组
# +-------------------+------------+------------------------------------+------------+--------+----+---------+--------+-----------+-------------+-------+--------+-----------------+-------------+
# |StartTime          |feIp        |QueryId                             |ConnectionId|Database|User|ScanBytes|ScanRows|MemoryUsage|DiskSpillSize|CPUTime|ExecTime|Warehouse        |ResourceGroup|
# +-------------------+------------+------------------------------------+------------+--------+----+---------+--------+-----------+-------------+-------+--------+-----------------+-------------+
# |2025-01-03 15:05:04|e08012839c6d|0ebba226-c9a1-11ef-824d-0242c0a83005|11          |        |root|0.000 B  |0 rows  |167.680 KB |0.000 B      |0.000 s|3.891 s |default_warehouse|bigQuery     |
# +-------------------+------------+------------------------------------+------------+--------+----+---------+--------+-----------+-------------+-------+--------+-----------------+-------------+
SHOW PROC '/current_queries';

# 3.针对已完成的查询，可以在审计日志中的【resourceGroup】字段查询到查询使用的资源组
# +------------------------------------+-------------------+----------+------------------+----+--------------+-------------+---------------+----+-----+-----------------------------------+---------+---------+--------+----------+-----------+------------+------+-------+------------+-------------------------------------------------------------------------------------+--------------------------------+------------+------------+
# |queryId                             |timestamp          |queryType |clientIp          |user|authorizedUser|resourceGroup|catalog        |db  |state|errorCode                          |queryTime|scanBytes|scanRows|returnRows|cpuCostNs  |memCostBytes|stmtId|isQuery|feIp        |stmt                                                                                 |digest                          |planCpuCosts|planMemCosts|
# +------------------------------------+-------------------+----------+------------------+----+--------------+-------------+---------------+----+-----+-----------------------------------+---------+---------+--------+----------+-----------+------------+------+-------+------------+-------------------------------------------------------------------------------------+--------------------------------+------------+------------+
# |0ebba226-c9a1-11ef-824d-0242c0a83005|2025-01-03 15:05:04|slow_query|192.168.48.1:62446|root|'root'@'%'    |bigQuery     |default_catalog|    |EOF  |                                   |10032    |9        |1       |1         |10001579552|186568      |826   |1      |e08012839c6d|/* ApplicationName=IntelliJ IDEA 2024.1.2 */ select name,sleep(10) from mydb.dict_tbl|85b40eb88f776ebf546bbeebeb2e819e|12          |0           |
# |dc2d95f0-c9a0-11ef-824d-0242c0a83005|2025-01-03 15:03:39|slow_query|192.168.48.1:62446|root|'root'@'%'    |default_mv_wg|default_catalog|    |EOF  |                                   |10018    |9        |1       |1         |10001064533|186568      |807   |1      |e08012839c6d|/* ApplicationName=IntelliJ IDEA 2024.1.2 */ select name,sleep(10) from mydb.dict_tbl|85b40eb88f776ebf546bbeebeb2e819e|12          |0           |
# +------------------------------------+-------------------+----------+------------------+----+--------------+-------------+---------------+----+-----+-----------------------------------+---------+---------+--------+----------+-----------+------------+------+-------+------------+-------------------------------------------------------------------------------------+--------------------------------+------------+------------+
select * from starrocks_audit_db__.starrocks_audit_tbl__ where queryType='slow_query' order by timestamp desc;

# 4.在审计日志中查询因为资源组熔断的任务
# +-------------------+------------------------------------+-------------+-----------------------------------+
# |timestamp          |queryId                             |resourceGroup|errorCode                          |
# +-------------------+------------------------------------+-------------+-----------------------------------+
# |2025-01-03 14:33:26|a336bf92-c99c-11ef-824d-0242c0a83005|bigQuery     |MEM_LIMIT_EXCEEDED                 |
# |2025-01-03 14:28:57|02e30ca1-c99c-11ef-824d-0242c0a83005|bigQuery     |BIG_QUERY_SCAN_ROWS_LIMIT_EXCEEDED |
# |2025-01-02 18:55:57|2584cf72-c8f8-11ef-824d-0242c0a83005|bigQuery     |MEM_LIMIT_EXCEEDED                 |
# |2025-01-02 18:53:24|ca24a0bf-c8f7-11ef-824d-0242c0a83005|bigQuery     |BIG_QUERY_SCAN_ROWS_LIMIT_EXCEEDED |
# |2025-01-02 18:46:13|c97e9275-c8f6-11ef-824d-0242c0a83005|bigQuery     |BIG_QUERY_SCAN_ROWS_LIMIT_EXCEEDED |
# |2025-01-02 18:42:43|4c55671e-c8f6-11ef-824d-0242c0a83005|bigQuery     |BIG_QUERY_SCAN_ROWS_LIMIT_EXCEEDED |
# |2025-01-02 18:28:54|5df24a94-c8f4-11ef-824d-0242c0a83005|bigQuery     |BIG_QUERY_CPU_SECOND_LIMIT_EXCEEDED|
# |2025-01-02 18:14:16|528b5b61-c8f2-11ef-824d-0242c0a83005|bigQuery     |BIG_QUERY_CPU_SECOND_LIMIT_EXCEEDED|
# |2025-01-02 18:00:15|5d79e3d3-c8f0-11ef-824d-0242c0a83005|bigQuery     |ANALYSIS_ERR                       |
# |2024-12-26 21:46:12|c4b96a4f-c38f-11ef-a41d-0242c0a83005|bigQuery     |BIG_QUERY_CPU_SECOND_LIMIT_EXCEEDED|
# |2024-12-26 21:39:15|cc2d0752-c38e-11ef-a41d-0242c0a83005|bigQuery     |BIG_QUERY_SCAN_ROWS_LIMIT_EXCEEDED |
# +-------------------+------------------------------------+-------------+-----------------------------------+
select timestamp,queryId,resourceGroup,errorCode from starrocks_audit_db__.starrocks_audit_tbl__ where state='ERR' and resourceGroup<>'' order by timestamp desc;



SHOW processlist;