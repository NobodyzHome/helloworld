# 【管理用户连接数】
# 我们可以给用户指定最大连接数，当超过该数量后，新的连接将会被拒绝，报以下错误。
# Reach user-level(qualifiedUser: root, currUserIdentity: 'root'@'%') connection limit, currentUserMaxConn=1, connectionMap. size=1, connByUser. totConn=1, user. currConn=1, node=e08012839c6d:9010
ALTER USER 'root' SET PROPERTIES ("max_user_connections" = "5");

# 【管理内存使用】
# query_mem_limit：单个查询的内存限制，单位是 Byte。建议设置为 17179869184（16GB）以上。
SET query_mem_limit = 1000;
# 当查询在执行中使用的内存超过query_mem_limit限制后，则会报以下错误。
# Memory of Query4b5bb092-d17a-11ef-b066-0242c0a83006 exceed limit. Pipeline Backend: starrocks-be-1, fragment: 4b5bb092-d17a-11ef-b066-0242c0a8300e Used: 454368, Limit: 1000. Mem usage has exceed the limit of single query, You can change the limit by set session variable query_mem_limit.: BE:10001
select birthday,count(distinct name) cnt from mydb.hello_world_source group by birthday;

# 【管理查询执行超时】
# 用于设置查询超时时间，单位为秒。该变量会作用于当前连接中所有的查询语句。自 v3.4.0 起，query_timeout 不再作用于 INSERT 语句。默认值：300 （5 分钟）
set query_timeout=2;
# 当查询的执行时间超过query_timeout限制后，则会报以下错误。
# Query exceeded time limit of 2 second
select birthday,count(distinct name) cnt from mydb.hello_world_source group by birthday;

# 【停止查询】
# 我们提交一个查询，让其休眠30s，模拟一个执行时间很长的查询。
select sleep(30);
# 打开一个新的客户端，使用show processlist可以查询到当前sr正在执行的sql。其中id字段就是该sql的connection id。
# +--+----+------------------+--+-------+-------------------+----+-----+---------------------------------------------------------------+---------+
# |Id|User|Host              |Db|Command|ConnectionStartTime|Time|State|Info                                                           |IsPending|
# +--+----+------------------+--+-------+-------------------+----+-----+---------------------------------------------------------------+---------+
# |16|root|192.168.48.1:56414|  |Query  |2025-01-13 15:47:10|5   |OK   |/* ApplicationName=IntelliJ IDEA 2024.3.1.1 */ select sleep(30)|false    |
# +--+----+------------------+--+-------+-------------------+----+-----+---------------------------------------------------------------+---------+
show processlist;
# 我们可以使用kill命令，将要停止的sql的connection id传入，来终止正在执行的查询。
kill 16;
# 当查询被手动终止后，客户端会收到以下错误。
# Can not read response from server. Expected to read 4 bytes, read 0 bytes before connection was unexpectedly lost.

