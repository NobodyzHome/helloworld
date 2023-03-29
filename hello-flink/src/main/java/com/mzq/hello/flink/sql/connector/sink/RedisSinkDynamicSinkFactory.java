package com.mzq.hello.flink.sql.connector.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.Set;

public class RedisSinkDynamicSinkFactory implements DynamicTableSinkFactory {

    public static ConfigOption<String> REDIS_URL_CONFIG = ConfigOptions.key("redis.url").stringType().noDefaultValue();

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);
        factoryHelper.validate();

        ReadableConfig readableConfig = factoryHelper.getOptions();
        return new RedisDynamicTableSink(readableConfig,context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(REDIS_URL_CONFIG);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
