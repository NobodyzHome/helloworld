package com.mzq.hello.flink.sql.connector.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TestLookupFunction extends LookupFunction {

    private DataType rootDataType;
    private String tableName;
    private Connection connection;
    private Table table;

    public TestLookupFunction(DataType rootDataType, String tableName) {
        this.rootDataType = rootDataType;
        this.tableName = tableName;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        Configuration configuration = new Configuration(true);
        connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration));
        table = connection.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void close() throws Exception {
        super.close();

        table.close();
        connection.close();
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        GenericRowData genericKeyRow = (GenericRowData) keyRow;
        List<String> fieldNames = DataType.getFieldNames(rootDataType);
        List<DataType> fieldDataTypes = DataType.getFieldDataTypes(rootDataType);
        GenericRowData rowData = new GenericRowData(RowKind.INSERT, fieldNames.size());

        Object idFieldValue = genericKeyRow.getField(0);
        String id = DataFormatConverters.getConverterForDataType(fieldDataTypes.get(0)).toExternal(idFieldValue).toString();

        Object nameFieldValue = genericKeyRow.getField(1);
        String name = DataFormatConverters.getConverterForDataType(fieldDataTypes.get(1)).toExternal(nameFieldValue).toString();

        Get get = new Get(Bytes.toBytes(id));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes(name));

        Result result = table.get(get);
        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes(name));
        String rst = null;
        if (Objects.nonNull(columnCells) && !columnCells.isEmpty()) {
            Cell cell = columnCells.get(0);
            rst = Bytes.toString(CellUtil.cloneValue(cell));
        }

        if (StringUtils.isNotBlank(rst)) {
            int i = 0;
            for (; i < keyRow.getArity(); i++) {
                Object fieldValue = genericKeyRow.getField(i);
                rowData.setField(i, fieldValue);
            }
            rowData.setField(i, DataFormatConverters.getConverterForDataType(fieldDataTypes.get(i)).toInternal(rst));
            return Collections.singleton(rowData);
        } else {
            return null;
        }
    }
}
