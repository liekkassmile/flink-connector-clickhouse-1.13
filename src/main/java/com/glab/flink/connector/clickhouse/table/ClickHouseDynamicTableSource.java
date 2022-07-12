package com.glab.flink.connector.clickhouse.table;

import com.glab.flink.connector.clickhouse.table.internal.ClickHouseRowDataLookupFunction;
import com.glab.flink.connector.clickhouse.table.internal.dialect.ClickHouseDialect;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.http.client.utils.URIBuilder;

/**
 * @author lrh
 * @date 2021/6/21
 */
public class ClickHouseDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsLimitPushDown {

    private final ResolvedSchema resolvedSchema;

    private final ClickHouseOptions options;

    private final JdbcLookupOptions lookupOptions;
    private long limit = -1;

    public ClickHouseDynamicTableSource(ResolvedSchema resolvedSchema, ClickHouseOptions options, JdbcLookupOptions lookupOptions) {
        this.resolvedSchema = resolvedSchema;
        this.options = options;
        this.lookupOptions = lookupOptions;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        String[] keyNames = new String[lookupContext.getKeys().length];
        for(int i = 0; i <keyNames.length; i++) {
            int[] innerKeyArr = lookupContext.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1, "JDBC only support non-nested look up keys");
            keyNames[i] = resolvedSchema.getColumnNames().get(innerKeyArr[0]);
        }

        final RowType rowType = (RowType)resolvedSchema.toSourceRowDataType().getLogicalType();
        ClickHouseRowDataLookupFunction lookupFunction =
                new ClickHouseRowDataLookupFunction(options, lookupOptions,
                        resolvedSchema.getColumnNames().stream().toArray(String[]::new),
                        resolvedSchema.getColumnDataTypes().stream().toArray(DataType[]::new), keyNames, rowType);
        return TableFunctionProvider.of(lookupFunction);
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }


    //仅供数据探查
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext){
        ClickHouseDialect dialect = (ClickHouseDialect)options.getDialect();
        String query = dialect.getSelectFromStatement(options.getTableName(), resolvedSchema.getColumnNames().stream().toArray(String[]::new), new String[0]);

        //1.13支持SupportsLimitPushDown，不然数据太大直接卡死了
        if(limit >= 0) {
            query = String.format("%s %s", query, dialect.getLimitClause(limit));
        }

        RowType rowType = (RowType)resolvedSchema.toSourceRowDataType().getLogicalType();
        getJdbcUrl(options.getUrl(), options.getDatabaseName());
        JdbcRowDataInputFormat build = JdbcRowDataInputFormat.builder()
                .setDrivername(options.getDialect().defaultDriverName().get())
                .setDBUrl(getJdbcUrl(options.getUrl(), options.getDatabaseName()))
                .setUsername(options.getUsername().orElse(null))
                .setPassword(options.getPassword().orElse(null))
                .setQuery(query)
                .setRowConverter(dialect.getRowConverter(rowType))
                .setRowDataTypeInfo(scanContext.createTypeInformation(resolvedSchema.toSourceRowDataType()))
                .build();
        return InputFormatProvider.of(build);
    }

    @Override
    public DynamicTableSource copy() {
        ClickHouseDynamicTableSource tableSource = new ClickHouseDynamicTableSource(resolvedSchema, options, lookupOptions);
        return tableSource;
    }

    @Override
    public String asSummaryString() {
        return "clickhouse source";
    }

    private String getJdbcUrl(String url, String dbName) {
        try {
            return "jdbc:" + (new URIBuilder(url)).setPath("/" + dbName).build().toString();
        }catch (Exception e) {
            throw new RuntimeException("get JDBC url failed.", e);
        }
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }
}
