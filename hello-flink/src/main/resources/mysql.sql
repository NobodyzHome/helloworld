show databases ;
use test_database;

insert into test_table values(2,'zhangsan',10),(3,'lisi',11),(4,'wangwu',12),(5,'zhaoliu',13),(6,'lili',14),(7,'nini',15);
update test_table set age=age+1;

delete from test_table;