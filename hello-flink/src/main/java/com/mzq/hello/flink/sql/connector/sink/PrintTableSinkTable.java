package com.mzq.hello.flink.sql.connector.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class PrintTableSinkTable implements DynamicTableSink {

    private DataType dataType;
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    public PrintTableSinkTable(DataType dataType, EncodingFormat encodingFormat) {
        this.dataType = dataType;
        this.encodingFormat = encodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> runtimeEncoder = encodingFormat.createRuntimeEncoder(context, dataType);
        return SinkFunctionProvider.of(new PrintTableSinkFunction(dataType, runtimeEncoder));
    }

    @Override
    public DynamicTableSink copy() {
        return new PrintTableSinkTable(dataType, encodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "tableSink";
    }
}
