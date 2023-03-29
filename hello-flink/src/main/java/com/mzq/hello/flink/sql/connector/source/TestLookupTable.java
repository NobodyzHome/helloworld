package com.mzq.hello.flink.sql.connector.source;

import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.types.DataType;

public class TestLookupTable implements LookupTableSource {

    private DataType dataType;
    private ResolvedCatalogTable catalogTable;

    public TestLookupTable(DataType dataType, ResolvedCatalogTable catalogTable) {
        this.dataType = dataType;
        this.catalogTable = catalogTable;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String tableName = catalogTable.getOptions().get("table-name");
        TestLookupFunction testLookupFunction = new TestLookupFunction(dataType, tableName);
        return LookupFunctionProvider.of(testLookupFunction);
    }

    @Override
    public DynamicTableSource copy() {
        return new TestLookupTable(dataType, catalogTable);
    }

    @Override
    public String asSummaryString() {
        return "test-lookup";
    }
}
