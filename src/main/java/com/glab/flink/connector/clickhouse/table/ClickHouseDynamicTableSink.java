package com.glab.flink.connector.clickhouse.table;

import com.glab.flink.connector.clickhouse.table.internal.AbstractClickHouseSinkFunction;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

public class ClickHouseDynamicTableSink implements DynamicTableSink {
    private final TableSchema tableSchema;

    private final ClickHouseOptions options;

    public ClickHouseDynamicTableSink(TableSchema tableSchema, ClickHouseOptions options) {
        this.tableSchema = tableSchema;
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
        Preconditions.checkState((ChangelogMode.insertOnly().equals(requestedMode) || this.tableSchema.getPrimaryKey().isPresent()), "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        AbstractClickHouseSinkFunction sinkFunction =
                (new AbstractClickHouseSinkFunction.Builder())
                        .withOptions(this.options)
                        .withFieldNames(this.tableSchema.getFieldNames())
                        .withFieldDataTypes(this.tableSchema.getFieldDataTypes())
                        //.withPrimaryKey(this.tableSchema.getPrimaryKey())
                        .withRowDataTypeInfo(context.createTypeInformation(this.tableSchema.toRowDataType()))
                        .build();
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public ClickHouseDynamicTableSink copy() {
        return new ClickHouseDynamicTableSink(this.tableSchema, this.options);
    }

    @Override
    public String asSummaryString() {
        return "ClickHouse sink";
    }
}
