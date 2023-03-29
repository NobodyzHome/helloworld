package com.mzq.hello.flink.sql.connector.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mzq.hello.flink.sql.connector.sink.RedisSinkDynamicSinkFactory.REDIS_URL_CONFIG;

public class RedisDynamicTableSink implements DynamicTableSink, SupportsWritingMetadata {

    private final ReadableConfig readableConfig;
    private final ResolvedSchema resolvedSchema;

    private List<String> metadataKeys;
    private DataType consumedDataType;

    public RedisDynamicTableSink(ReadableConfig readableConfig, ResolvedSchema resolvedSchema) {
        this.readableConfig = readableConfig;
        this.resolvedSchema = resolvedSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.all();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        String url = readableConfig.get(REDIS_URL_CONFIG);

        Integer[] metaPosition = Arrays.stream(RedisMetadata.values()).map(meta -> {
            int indexOf = metadataKeys.indexOf(meta.getKey());
            return indexOf < 0 ? -1 : resolvedSchema.toPhysicalRowDataType().getChildren().size() + indexOf;
        }).toArray(Integer[]::new);
        RedisSink redisSink = new RedisSink(url, metaPosition);
        return SinkV2Provider.of(redisSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(readableConfig, resolvedSchema);
    }

    @Override
    public String asSummaryString() {
        return "redis sink table";
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return Arrays.stream(RedisMetadata.values()).collect(Collectors.toMap(RedisMetadata::getKey, RedisMetadata::getDataType));
    }

    /**
     * flink会重新将sink table中的字段重新整理，先将物理字段放在前面，再将metadata字段追加到后面，使字段按类型有序。此后sink中收到的RowData数据中，就是按重新整理后的字段顺序来存储的
     * List<String> metadataKeys则是字段重新排序完以后的每一个meta字段的metadata key。
     * 我们遍历每一个metadata key，用index + resolvedSchema.toPhysicalRowDataType().getChildren().size()就可以获取到每一个metadata key在整理后的字段列表的位置，也就是每一个metadata column的位置
     * 该方法会在getSinkRuntimeProvider方法之前被执行。
     * <p>
     * 什么是metadata key？
     * 我们在create table时，可以将column设置为metadata，例如ttl bigint metadata from 'the_ttl'，其中ttl是metadata字段，the_ttl是这个字段对应的metadata_key
     */
    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
        this.consumedDataType = consumedDataType;
    }

    enum RedisMetadata {
        TEST("test", DataTypes.VARCHAR(10)),
        TTL("the_ttl", DataTypes.INT());

        private final String key;
        private final DataType dataType;

        RedisMetadata(String key, DataType dataType) {
            this.key = key;
            this.dataType = dataType;
        }

        public String getKey() {
            return key;
        }

        public DataType getDataType() {
            return dataType;
        }
    }
}
