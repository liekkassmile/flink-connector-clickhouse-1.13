package com.glab.flink.connector.clickhouse.table.internal.executor;

import com.glab.flink.connector.clickhouse.table.internal.ClickHouseStatementFactory;
import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Optional;

public interface ClickHouseExecutor extends Serializable {
    void prepareStatement(ClickHouseConnection clickHouseConnection) throws SQLException;

    void prepareStatement(ClickHouseConnectionProvider clickHouseConnectionProvider) throws SQLException;

    void setRuntimeContext(RuntimeContext context);

    void addBatch(RowData rowData) throws IOException;

    void executeBatch() throws IOException;

    void closeStatement() throws SQLException;

    String getState();

    static ClickHouseUpsertExecutor createUpsertExecutor(String tableName,
                                                         String[] fieldNames,
                                                         String[] keyFields,
                                                         ClickHouseRowConverter converter,
                                                         ClickHouseOptions options) {
        String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
        String updateSql = ClickHouseStatementFactory.getUpdateStatement(tableName, fieldNames, keyFields,
                Optional.empty());
        String deleteSql = ClickHouseStatementFactory.getDeleteStatement(tableName, keyFields, Optional.empty());
        return new ClickHouseUpsertExecutor(insertSql, updateSql, deleteSql, converter, options);
    }
}
