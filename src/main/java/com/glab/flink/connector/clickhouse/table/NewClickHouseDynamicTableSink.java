package com.glab.flink.connector.clickhouse.table;

import com.glab.flink.connector.clickhouse.table.internal.AbstractClickHouseSinkFunction;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;


public class NewClickHouseDynamicTableSink implements DynamicTableSink {
    private final ResolvedSchema resolvedSchema;

    private final ClickHouseOptions options;

    public NewClickHouseDynamicTableSink(ResolvedSchema resolvedSchema, ClickHouseOptions options) {
        this.resolvedSchema = resolvedSchema;
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        Preconditions.checkState((ChangelogMode.insertOnly().equals(requestedMode) || this.resolvedSchema.getPrimaryKey().isPresent()), "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AbstractClickHouseSinkFunction sinkFunction =
                (new AbstractClickHouseSinkFunction.Builder())
                        .withOptions(this.options)
                        .withFieldNames(this.resolvedSchema.getColumnNames().stream().toArray(String[]::new))
                        .withFieldDataTypes(this.resolvedSchema.getColumnDataTypes().stream().toArray(DataType[]::new))
                        .withPrimaryKey(this.resolvedSchema.getPrimaryKey())
                        .withRowDataTypeInfo(context.createTypeInformation(this.resolvedSchema.toSinkRowDataType()))
                        .build();
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public NewClickHouseDynamicTableSink copy() {
        return new NewClickHouseDynamicTableSink(this.resolvedSchema, this.options);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse sink";
    }
}
