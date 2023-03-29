package com.mzq.hello.flink.sql.connector.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class PrintTableSinkFunction extends RichSinkFunction<RowData> {

    private DataType dataType;
    private SerializationSchema<RowData> serializationSchema;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        serializationSchema.open(null);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    public PrintTableSinkFunction(DataType dataType, SerializationSchema<RowData> serializationSchema) {
        this.dataType = dataType;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        System.out.println(new String(serializationSchema.serialize(value)));
    }
}
