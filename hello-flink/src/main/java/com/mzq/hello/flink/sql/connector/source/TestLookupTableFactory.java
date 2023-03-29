package com.mzq.hello.flink.sql.connector.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.Collections;
import java.util.Set;

public class TestLookupTableFactory implements DynamicTableSourceFactory {
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        return new TestLookupTable(context.getPhysicalRowDataType(), catalogTable);
    }

    @Override
    public String factoryIdentifier() {
        return "testLookup";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(ConfigOptions.key("table-name").stringType().noDefaultValue());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
