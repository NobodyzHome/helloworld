# 当我们通过审计日志发现一些大查询会严重影响集群的性能，长期方案肯定是优化sql，但眼下重要的是限制这些sql，不让他们继续对集群造成更大的压力。此时可以使用黑名单机制，当执行的sql与黑名单的正则匹配，则会拒绝执行该sql。
# 黑名单机制默认是不启动的，需要手动设置启动
ADMIN SET FRONTEND CONFIG ("enable_sql_blacklist" = "true");

# 增加黑名单。注意：黑名单中内容为正则表达式，只要这个正则表达式能够匹配上要执行的sql（不需要和sql完全一致），就可以拒绝该sql的执行。
# 增加一个黑名单，拒绝访问mydb.hello_world表的sql
ADD SQLBLACKLIST "mydb.hello_world";
# 增加一个黑名单，拒绝使用count(distinct)的sql
ADD SQLBLACKLIST "count\\(distinct .+\\)";
# 增加一个黑名单，拒绝limit在2000到3000以内的sql
ADD SQLBLACKLIST "limit [2000,3000]";

# 查询所有的黑名单
# +--+--------------------+
# |Id|Forbidden SQL       |
# +--+--------------------+
# |6 |limit [2000,3000]   |
# |7 |mydb.hello_world    |
# |8 |count\(distinct .+\)|
# +--+--------------------+
SHOW SQLBLACKLIST;

# 删除一个黑名单，传入的值是黑名单的id
DELETE SQLBLACKLIST 6;

# 当执行的sql匹配上黑名单，则会报报错：Access denied; This sql is in blacklist, please contact your admin
select * from mydb.hello_world limit 1;
select uid,count(distinct age) from mydb.hello_world_source group by uid;

# 注意：假设黑名单中有mydb.hello_world这个配置，我们的本意是只想限制hello_world表的查询，但库里还有个hello_world_source，也能和这个正则匹配上，导致hello_world_source表也不能被查询了。
select * from mydb.hello_world_source limit 1500;
# 此时我们可以去除mydb.hello_world这个黑名单，增加以下两个黑名单。代表只限制hello_world表。
DELETE SQLBLACKLIST 7;
ADD SQLBLACKLIST "mydb.hello_world where";
ADD SQLBLACKLIST "mydb.hello_world$";
# 此时以下两种sql都无法执行了
select * from mydb.hello_world where age=1;
select * from mydb.hello_world;

# 注意：目前黑名单仅对select语句有效。因此下面这个sql语句，尽管能和黑名单匹配上，也不会被拒绝执行。
insert into mydb.hello_world values(9999,'hello',11,1,1000,15,22);