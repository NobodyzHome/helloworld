package com.mzq.usage.hadoop.hbase;

import com.mzq.usage.Employee;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class HbaseHelloWorld {

    public static void main(String[] args) throws IOException {
//        // 创建hadoop的configuration对象，会把classpath下的core-site.xml、hdfs-site.xml、mapred-site.xml、yarn-site.xml的内容加载进来
//        Configuration configuration = new Configuration(true);
//        // 在configuration对象基础上，再把classpath下的hbase-site.xml的内容加载进来
//        Configuration hBaseConfiguration = HBaseConfiguration.create(configuration);
//        /*
//         * 创建hbase client需要给出zookeeper地址
//         *
//         * 同时，hbase client需要能够连接到：
//         * 1.zookeeper
//         * 2.hbase-master
//         * 3.所有的hbase-region
//         */
//        Connection connection = ConnectionFactory.createConnection(hBaseConfiguration);
//        // 创建Admin对象，用于ddl的操作
//        Admin admin = connection.getAdmin();
//        TableName[] tableNames = admin.listTableNames();
//        System.out.println(tableNames);
//        Table dept = connection.getTable(TableName.valueOf("dept"));
//        Get get = new Get(Bytes.toBytes("1000"));
//        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
//        Result result = dept.get(get);
//        System.out.println(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));

//        ddl();
//        dml();
//        dmlScanner();
//        modifyColumn();
        dmlDeleteFamily();
//        dmlDeleteColumns();
//        dmlDeleteColumn();
    }

    private static void ddl() {
        Configuration configuration = new Configuration(true);
        Configuration hBaseConfiguration = HBaseConfiguration.create(configuration);
        try (Connection connection = ConnectionFactory.createConnection(hBaseConfiguration);
             Admin admin = connection.getAdmin()) {
            TableName[] tableNames = admin.listTableNames();
            log.info("tableNames={}", Arrays.stream(tableNames).map(TableName::getNameAsString).collect(Collectors.joining(",")));

            TableName tableName = TableName.valueOf("helloApi");
            boolean tableExists = admin.tableExists(tableName);
            if (tableExists) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("info1"));
            hTableDescriptor.addFamily(new HColumnDescriptor("info2"));

            admin.createTable(hTableDescriptor);
            TableName[] listTableNames = admin.listTableNames();
            log.info("tableNames={}", Arrays.stream(listTableNames).map(TableName::getNameAsString).collect(Collectors.joining(",")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void modifyColumn() {
        Configuration configuration = new Configuration(true);
        Configuration hbaseConfiguration = HBaseConfiguration.create(configuration);
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf("dept");
            HTableDescriptor dept = admin.getTableDescriptor(tableName);
            HColumnDescriptor[] columnFamilies = dept.getColumnFamilies();

            for (HColumnDescriptor hColumnDescriptor : columnFamilies) {
                log.info("column={},maxVersion={},ttl={}", hColumnDescriptor.getNameAsString(), hColumnDescriptor.getMaxVersions(), hColumnDescriptor.getTimeToLive());

                hColumnDescriptor.setTimeToLive(Integer.MAX_VALUE);
                hColumnDescriptor.setMaxVersions(3);
                admin.modifyColumn(tableName, hColumnDescriptor);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dml() {
        Configuration configuration = new Configuration(true);
        Configuration hbaseConfiguration = HBaseConfiguration.create(configuration);
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf("helloApi");
            boolean tableExists = admin.tableExists(tableName);
            if (tableExists) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("info1").setMaxVersions(3));
            // 在设置列族时，还可以设置列族的ttl、maxVersion等属性
            hTableDescriptor.addFamily(new HColumnDescriptor("info2").setMaxVersions(2).setTimeToLive(60));
            admin.createTable(hTableDescriptor);

            try (Table table = connection.getTable(tableName)) {
                List<Put> list = new ArrayList<>(10000);
                for (int i = 1; i <= 10000; i++) {
                    Employee generate = Employee.generate();
                    // 一次put可以对rowkey下的多个列族的字段进行设置
                    Put put = new Put(Bytes.toBytes(generate.getEmp_no()));
                    put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("emp_name"), Bytes.toBytes(generate.getEmp_name()));
                    put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("dept_no"), Bytes.toBytes(generate.getDept_no()));
                    put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("dept_name"), Bytes.toBytes(generate.getDept_name()));
                    list.add(put);
                }
                table.put(list);

                ResultScanner scanner = table.getScanner(new Scan());
                // 每一个result代表一个rowkey下的多个列族的cell
                for (Result result : scanner) {
                    String row = Bytes.toString(result.getRow());
                    CellScanner cellScanner = result.cellScanner();
                    while (cellScanner.advance()) {
                        Cell cell = cellScanner.current();
                        String family = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                        long cellTimestamp = cell.getTimestamp();
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        log.info("row={},family:qualifier={}:{},timestamp={},value={}", row, family, qualifier, cellTimestamp, value);
                    }
                }

                Put put = new Put(Bytes.toBytes("1001"));
                put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("zhaoliu"));
                put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("tel"), Bytes.toBytes("188"));

                long timestamp = System.currentTimeMillis();
                Put put1 = new Put(Bytes.toBytes("1001"));
                put1.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), timestamp - 3000, Bytes.toBytes("lisi"));

                Put put2 = new Put(Bytes.toBytes("1001"));
                put2.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), timestamp + 3000, Bytes.toBytes("wangwu"));

                table.put(Arrays.asList(put, put1, put2));

                // 在建表时，如果列族设置了存储的最大版本数，那么列族会存储多个版本。在get请求里可以设置最大版本数，拉取列族存储的所有版本里前x个版本的数据
                Get get = new Get(Bytes.toBytes("1001")).addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name")).setMaxVersions(2);
                Result result = table.get(get);
                long minTs = Long.MAX_VALUE;
                for (Cell cell : result.listCells()) {
                    String row = Bytes.toString(CellUtil.cloneRow(cell));
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    long cellTimestamp = cell.getTimestamp();
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    log.info("row={},family:qualifier={}:{},timestamp={},value={}", row, family, qualifier, cellTimestamp, value);
                    minTs = Math.min(minTs, cellTimestamp);
                }

                Delete delete = new Delete(Bytes.toBytes("1001"), minTs + 3000);
                delete.addColumns(Bytes.toBytes("info1"), Bytes.toBytes("name"));
                table.delete(delete);

                Get get1 = new Get(Bytes.toBytes("1001"));
                get1.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
                Result result1 = table.get(get1);
                assert result1.listCells() == null || result1.listCells().isEmpty();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            admin.flush(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dmlScanner() {
        Configuration configuration = new Configuration(true);
        Configuration hbaseConfiguration = HBaseConfiguration.create(configuration);
        TableName tableName = TableName.valueOf("helloApi");
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
             Table table = connection.getTable(tableName)) {
            List<Put> putList = new ArrayList<>(20000);
            for (int i = 1; i <= 20000; i++) {
                Employee employee = Employee.generate();
                Put put = new Put(Bytes.toBytes(employee.getEmp_no()));
                put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("emp_name"), Bytes.toBytes(employee.getEmp_name()));
                put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("sex"), Bytes.toBytes(employee.getSex()));
                put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("create_dt"), Bytes.toBytes(employee.getCreate_dt()));
                put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("dept_no"), Bytes.toBytes(employee.getDept_no()));
                put.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("dept_name"), Bytes.toBytes(employee.getDept_name()));
                putList.add(put);
            }
            table.put(putList);

            Scan scan = new Scan();
            // 设置本次扫描的列族和字段
            scan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("emp_name"));
            // 设置本次扫描从哪个rowkey开始
            scan.setStartRow(putList.get(10).getRow());

            ResultScanner scanner = table.getScanner(scan);
            // 每一个result代表一个rowkey下的多个列族的cell
            for (Result result : scanner) {
                String row = Bytes.toString(result.getRow());
                // 使用迭代器模式来遍历一个result下的所有cell（这些cell可能是不同列族，也可能是相同列族的）
                CellScanner cellScanner = result.cellScanner();
                while (cellScanner.advance()) {
                    Cell cell = cellScanner.current();
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    long cellTimestamp = cell.getTimestamp();
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    log.info("row={},family:qualifier={}:{},timestamp={},value={}", row, family, qualifier, cellTimestamp, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dmlDeleteFamily() {
        Configuration configuration = new Configuration(true);
        Configuration hbaseConfiguration = HBaseConfiguration.create(configuration);
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
             Table table = connection.getTable(TableName.valueOf("dept"))) {
            for (int i = 1; i <= 5; i++) {
                Put put = new Put(Bytes.toBytes("2010"));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(RandomStringUtils.random(5, true, false)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(RandomUtils.nextInt(20, 40))));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(new String[]{"female", "male"}[RandomUtils.nextInt(0, 2)]));
                put.addColumn(Bytes.toBytes("tmp"), Bytes.toBytes("level"), Bytes.toBytes(String.valueOf(RandomUtils.nextInt(0, 10))));
                put.addColumn(Bytes.toBytes("tmp"), Bytes.toBytes("salary"), Bytes.toBytes(String.valueOf(RandomUtils.nextInt(2000, 9000))));
                table.put(put);
            }

            Get get = new Get(Bytes.toBytes("2010"));
            get.addFamily(Bytes.toBytes("info"));
            get.addFamily(Bytes.toBytes("tmp"));
            Result result = table.get(get);
            CellScanner cellScanner = result.cellScanner();

            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                long cellTimestamp = cell.getTimestamp();
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                log.info("row={},family:qualifier={}:{},timestamp={},value={}", row, family, qualifier, cellTimestamp, value);
            }

            Delete delete = new Delete(Bytes.toBytes("2010"));
            // 在delete.addFamily相当于命令行中的deleteall，但与命令行不同的是，api中可以删除单一一个column，而命令行中只能删除所有的column
            delete.addFamily(Bytes.toBytes("info"));
            table.delete(delete);

            // 删除列族后，再查询该rowkey的该列族，就查不到数据了
            Get get1 = new Get(Bytes.toBytes("2010"));
            get1.addFamily(Bytes.toBytes("info"));
            Result result1 = table.get(get1);
            assert result1.listCells() == null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dmlDeleteColumns() {
        Configuration configuration = new Configuration(true);
        Configuration hBaseConfiguration = HBaseConfiguration.create(configuration);
        try (Connection connection = ConnectionFactory.createConnection(hBaseConfiguration);
             Table table = connection.getTable(TableName.valueOf("dept"))) {
            for (int i = 1; i <= 10; i++) {
                Put put = new Put(Bytes.toBytes("2006"));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(RandomStringUtils.random(5, true, false)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(RandomUtils.nextInt(20, 40))));
                table.put(put);
            }

            Get get1 = new Get(Bytes.toBytes("2006"));
            get1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
            get1.setMaxVersions(10);
            Scan scan = new Scan(get1);
            ResultScanner scanner = table.getScanner(scan);
            Result result1 = scanner.next();
            // 获取2006,info:name下的最新的cell的timestamp，用于后面设置删除标记的timestamp
            long timestamp = result1.listCells().get(0).getTimestamp();

            Delete delete = new Delete(Bytes.toBytes("2006"));
            // 使用addColumns方法，而不是addColumn方法。添加的是DeleteColumn标识的数据，该数据以为着删除这个column的version下面的所有的cell
            delete.addColumns(Bytes.toBytes("info"), Bytes.toBytes("name"), timestamp - 5);
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dmlDeleteColumn() {
        Configuration configuration = new Configuration(true);
        Configuration hbaseConfiguration = HBaseConfiguration.create(configuration);
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
             Table table = connection.getTable(TableName.valueOf("dept"))) {
            for (int i = 1; i <= 5; i++) {
                Put put = new Put(Bytes.toBytes("2007"));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(RandomStringUtils.random(5, true, false)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(RandomUtils.nextInt(20, 40))));

                table.put(put);
            }

            // 获取2007,info:name的正数第二个cell的timestamp
            Get get = new Get(Bytes.toBytes("2007"));
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")).setMaxVersions(30);
            Result result = table.get(get);
            Cell cell = result.listCells().get(1);

            // 仅删除2007,info:name的正数第2个cell，对于第3个、第4个等以后的cell不受影响
            Delete delete = new Delete(Bytes.toBytes("2007"));
            // 在这里使用的是addColumn方法，添加的数据是Delete删除标识，而不是DeleteColumn标识
            delete.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp());
            table.delete(delete);

            Get get1 = new Get(Bytes.toBytes("2007"));
            get1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")).setMaxVersions(3);
            Result result1 = table.get(get1);
            for (Cell cell1 : result1.listCells()) {
                assert cell1.getTimestamp() != cell.getTimestamp();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
