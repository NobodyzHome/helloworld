# 资源组和查询队列可以主动防御大查询，但也有可能有些大查询绕过了防御机制。此时我们就需要通过查询记录看集群执行了哪些大查询，并加以预防。
# 【监控正在执行的大查询】
# 我们可以使用下面命令来查询当前正在执行的sql以及sql的执行时长ExecTime
# +-------------------+------------+------------------------------------+------------+--------+----+---------+--------+-----------+-------------+-------+--------+-----------------+-------------+
# |StartTime          |feIp        |QueryId                             |ConnectionId|Database|User|ScanBytes|ScanRows|MemoryUsage|DiskSpillSize|CPUTime|ExecTime|Warehouse        |ResourceGroup|
# +-------------------+------------+------------------------------------+------------+--------+----+---------+--------+-----------+-------------+-------+--------+-----------------+-------------+
# |2025-01-15 18:09:25|e08012839c6d|cc4d120d-d328-11ef-b066-0242c0a83006|29          |        |root|0.000 B  |0 rows  |54.391 KB  |0.000 B      |0.000 s|3.445 s |default_warehouse|default_wg   |
# +-------------------+------------+------------------------------------+------------+--------+----+---------+--------+-----------+-------------+-------+--------+-----------------+-------------+
show proc '/current_queries';
# 针对执行时间长的查询任务，我们可以通过show proc '/current_queries/QueryId/hosts'来获取该任务在每个BE上的资源使用情况;
# +-------------------+---------+--------+--------------+-------------+
# |Host               |ScanBytes|ScanRows|CpuCostSeconds|MemUsageBytes|
# +-------------------+---------+--------+--------------+-------------+
# |starrocks-be-0:8060|0.000 B  |0 rows  |0.000 s       |54.391 KB    |
# +-------------------+---------+--------+--------------+-------------+
SHOW PROC '/current_queries/cc4d120d-d328-11ef-b066-0242c0a83006/hosts';
# 我们拿到大sql的queryId后，可以从审计日志中获取具体的查询语句
# +---------------------------------------------------------------+
# |stmt                                                           |
# +---------------------------------------------------------------+
# |/* ApplicationName=IntelliJ IDEA 2024.3.1.1 */ select sleep(30)|
# +---------------------------------------------------------------+
select stmt from starrocks_audit_db__.starrocks_audit_tbl__ where queryId='cc4d120d-d328-11ef-b066-0242c0a83006';
# 我们可以通过kill命令，结合show proc '/current_queries'中返回的【ConnectionId】，来停止正在运行的大查询
kill query 29;

# 【监控已查询完成的大查询】
# 通过审计日志可以查询到出现次数最多的大查询语句
# +------------------------------------------------------------------------------------------------------------------------+---+--------------+--------------+--------------+--------------+
# |stmt                                                                                                                    |cnt|min_query_time|max_query_time|max_query_time|avg_query_time|
# +------------------------------------------------------------------------------------------------------------------------+---+--------------+--------------+--------------+--------------+
# |/* ApplicationName=IntelliJ IDEA 2024.3.1.1 */ select sleep(30)                                                         |8  |29987         |30028         |30028         |29995         |
# |/* ApplicationName=IntelliJ IDEA 2024.3.1.1 */ select uid,count(distinct name) from mydb.hello_world_source group by uid|2  |5234          |6736          |6736          |5985          |
# +------------------------------------------------------------------------------------------------------------------------+---+--------------+--------------+--------------+--------------+
SELECT
    stmt,
    COUNT( *) cnt,
    MIN(queryTime) min_query_time,
    MAX(queryTime) max_query_time,
    MAX(queryTime) max_query_time,
    ROUND(AVG(queryTime)) avg_query_time
FROM
    starrocks_audit_db__.starrocks_audit_tbl__
WHERE
    TIMESTAMP >= curdate() - interval 5 DAY
  AND queryType = 'slow_query'
GROUP BY
    stmt
ORDER BY
    cnt DESC;

# 【优化大查询】
# 要优化大查询，需要让sr抓取大查询的profile，分析哪块执行耗时。由于sr抓取查询的profile也很耗费资源，所以不要对每个查询都抓取profile，只对大查询抓取profile。
# 尽量不要启动enable_profile，因为启动后，每一个查询都会被记录执行过程，这对sr集群也是不小的负担。
set enable_profile=true;
# 我们可以启动big_query_profile_threshold，只针对执行时间超过big_query_profile_threshold的才进行profile的抓取。
set big_query_profile_threshold=3s;
# 执行一个时间较长的sql
# 500 rows retrieved starting from 1 in 3 s 481 ms (execution: 3 s 319 ms, fetching: 162 ms)
select uId,count(distinct name) from mydb.hello_world_source group by uId;
# 使用show profilelist获取抓到的慢查询的profile
# +------------------------------------+-------------------+-------+--------+------------------------------------------------------------------------------------------------------------------------+
# |QueryId                             |StartTime          |Time   |State   |Statement                                                                                                               |
# +------------------------------------+-------------------+-------+--------+------------------------------------------------------------------------------------------------------------------------+
# |16f5680a-d32f-11ef-b066-0242c0a83006|2025-01-15 18:54:27|3s366ms|Finished|/* ApplicationName=IntelliJ IDEA 2024.3.1.1 */ select uId,count(distinct name) from mydb.hello_world_source group by uId|
# |8d3e1676-d32d-11ef-b066-0242c0a83006|2025-01-15 18:43:26|5s339ms|Finished|/* ApplicationName=IntelliJ IDEA 2024.3.1.1 */ select uid,count(distinct name) from mydb.hello_world_source group by uid|
# +------------------------------------+-------------------+-------+--------+------------------------------------------------------------------------------------------------------------------------+
show profilelist;
# 我们可以通过analyze porfile获取指定profile的分析，包括最耗时的操作、最耗内存的操作等。以此来决定如何对sql进行优化
# Top Most Time-consuming Nodes:
# 1. AGGREGATION (id=3) [merge, serialize]: 2s798ms (90.23%)
# 2. EXCHANGE (id=2) [SHUFFLE]: 215.501ms (6.95%)
# 3. AGGREGATION (id=4) [finalize, update]: 80.208ms (2.59%)
# 4. EXCHANGE (id=5) [GATHER]: 5.741ms (0.19%)
# 5. RESULT_SINK: 538.383us (0.02%)
# 6. OLAP_SCAN (id=0) : 0ns (0.00%)
# 7. AGGREGATION (id=1) [serialize, update]: 0ns (0.00%)
#
# Top Most Memory-consuming Nodes:
# 1. AGGREGATION (id=3) [merge, serialize]: 339.622 MB
# 2. EXCHANGE (id=2) [SHUFFLE]: 18.807 MB
# 3. AGGREGATION (id=4) [finalize, update]: 552.563 KB
# 4. EXCHANGE (id=5) [GATHER]: 53.318 KB
analyze profile from '16f5680a-d32f-11ef-b066-0242c0a83006';
