create table employee(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
row format DELIMITED fields terminated by ',' lines terminated by '\n'
stored as textfile;
load data inpath '/data/employee' overwrite into table employee;

truncate table employee;

-- 让hive启动压缩，默认为false，如果需要压缩，则该属性要设置为true
set hive.exec.compress.output=true;
-- 设置MapTask执行结果落盘时，是否需要对落盘的数据进行压缩
set mapreduce.map.output.compress=true;
-- 设置MapTask对落盘数据压缩时，使用的压缩技术
set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.DefaultCodec;
-- 设置MapReduce Job最终的计算结果落盘时是否需要压缩
set mapreduce.output.fileoutputformat.compress=true;
-- 设置MapReduce Job最终的计算结果落盘时使用的压缩技术
-- set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.DefaultCodec;
set mapreduce.job.max.map=1;
set mapreduce.job.reduces=1;
-- 取消fetch模式，也就是即使单纯select一张表，也要走mapreduce
set hive.fetch.task.conversion=none;
set mapreduce.input.fileinputformat.split.maxsize=5000;

-- 验证可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
insert overwrite  table employee_textfile_zlib select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;

-- 验证textfile + gzip：未压缩：32mb，压缩后：6mb。压缩后的文件在读取时【不可】被切分成多个split。
create table employee_textfile_gzip(
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
set mapreduce.input.fileinputformat.split.maxsize=50000000;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
insert overwrite  table employee_textfile_gzip select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：不可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select * from employee_textfile_gzip limit 1;

-- 验证textfile + deflate：未压缩：32mb，压缩后：6mb。压缩后的文件在读取时【不可】被切分成多个split。
create table employee_textfile_deflate(
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
-- 设置mapreduce job输出的文件的压缩格式
set mapreduce.input.fileinputformat.split.maxsize=50000000;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.DeflateCodec;
insert overwrite table employee_textfile_deflate select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：不可切分
-- 将每个split最大大小设置很小，看是否在查询时生成多个map task，也就是验证压缩后的文件是否可以切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select  * from employee_textfile_deflate limit 1;
-- docker compose exec resourcemanager  /opt/hadoop-2.7.4/bin/hadoop job  -kill job_1667030402681_0006

-- 验证textfile + bzip2：未压缩：32mb，压缩后：6mb。压缩后的文件在读取时【可以】被切分成多个split。
create table employee_textfile_bzip2(
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
set mapreduce.input.fileinputformat.split.maxsize=50000000;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;
insert overwrite table employee_textfile_bzip2 select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select * from employee_textfile_bzip2 limit 1;

-- 验证textfile + lz4：未压缩：32mb，压缩后：10mb。压缩后的文件在读取时【不可以】被切分成多个split。
create table employee_textfile_lz4(
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
set hive.exec.compress.output=true;
set mapreduce.input.fileinputformat.split.maxsize=50000000;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.Lz4Codec;
insert overwrite table employee_textfile_lz4 select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：不可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select * from employee_textfile_lz4 limit 1;


-- 验证sequencefile：相同数据内容，textfile：32mb，sequencefile：39mb。生成的文件在读取时【可以】被切分成多个split。
create table employee_sequencefile(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
row format delimited fields terminated by ',' lines terminated by '\n'
stored as sequencefile ;

set hive.exec.compress.output=false;
set mapreduce.input.fileinputformat.split.maxsize=50000000;
insert overwrite table employee_sequencefile select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select * from employee_sequencefile limit 1;

-- 验证orc文件格式：textfile大小：32mb，orc大小：7.32mb，orc文件在读取时【可以】被切分
create table employee_orc(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
stored as orc
-- orc是使用tblproperties来设置压缩技术，orc可以使用以下压缩技术：zlib、snappy
tblproperties (
    'orc.compress'='NONE'
    );

set hive.exec.compress.output=false;
set mapreduce.input.fileinputformat.split.maxsize=50000000;
insert overwrite table employee_orc select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select * from employee_orc limit 1;

-- 验证orc + zlib:压缩前：7.32mb，压缩后：4.32。zlib压缩后的orc文件是【可以】被切分的
create table employee_orc_zlib(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
stored as orc
tblproperties(
    'orc.compress'='ZLIB'
);
set mapreduce.input.fileinputformat.split.maxsize=50000000;
insert overwrite table employee_orc_zlib select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select * from employee_orc_zlib limit 1;

-- 验证parquet：textfile大小：32mb，parquest大小：9mb。parquet文件【可以】被切分
create table employee_parquet(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
stored as parquet
tblproperties (
    'parquet.compression'='UNCOMPRESSED' -- you can use none, gzip, snappy https://github.com/apache/parquet-format/blob/master/Compression.md
);
-- parquet有以下压缩格式：参见org.apache.flink.hive.shaded.parquet.hadoop.metadata.CompressionCodecName类
-- UNCOMPRESSED((String)null, CompressionCodec.UNCOMPRESSED, ""),
-- SNAPPY("org.apache.flink.hive.shaded.parquet.hadoop.codec.SnappyCodec", CompressionCodec.SNAPPY, ".snappy"),
-- GZIP("org.apache.hadoop.io.compress.GzipCodec", CompressionCodec.GZIP, ".gz"),
-- LZO("com.hadoop.compression.lzo.LzoCodec", CompressionCodec.LZO, ".lzo"),
-- BROTLI("org.apache.hadoop.io.compress.BrotliCodec", CompressionCodec.BROTLI, ".br"),
-- LZ4("org.apache.hadoop.io.compress.Lz4Codec", CompressionCodec.LZ4, ".lz4"),
-- ZSTD("org.apache.flink.hive.shaded.parquet.hadoop.codec.ZstandardCodec", CompressionCodec.ZSTD, ".zstd");
set hive.exec.compress.output=false;
set mapreduce.input.fileinputformat.split.maxsize=50000000;
insert overwrite table employee_parquet select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select * from employee_parquet limit 1;

-- 验证parquet + gzip：压缩前大小：9mb，压缩后大小：4.03mb。gzip压缩后的parquet文件【可以】被切分
create table employee_parquet_gzip(
    emp_no string,
    emp_name string,
    dept_no string,
    dept_name string,
    sex string,
    create_dt string,
    salary int
)
stored as parquet
tblproperties (
    'parquet.compression'='GZIP'
    );

set mapreduce.input.fileinputformat.split.maxsize=50000000;
insert overwrite table employee_parquet_gzip select emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary from employee group by emp_no,emp_name,dept_no,dept_name,sex,create_dt,salary;
-- 验证是否可切分：可切分
set mapreduce.input.fileinputformat.split.maxsize=5000;
select * from employee_parquet_gzip limit 1;