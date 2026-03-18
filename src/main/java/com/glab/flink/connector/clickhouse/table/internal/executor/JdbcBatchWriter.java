package com.glab.flink.connector.clickhouse.table.internal.executor;

import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.sql.SQLException;
import java.util.List;

public class JdbcBatchWriter implements ClickHouseBulkWriter {
    private final String sql;
    private final ClickHouseRowConverter converter;
    private final ClickHouseConnectionProvider connectionProvider;
    private final ClickHouseConnection directConnection;

    private ClickHouseConnection connection;
    private ClickHousePreparedStatement statement;

    public JdbcBatchWriter(String sql,
                           ClickHouseRowConverter converter,
                           ClickHouseConnectionProvider connectionProvider,
                           ClickHouseConnection directConnection) {
        this.sql = sql;
        this.converter = converter;
        this.connectionProvider = connectionProvider;
        this.directConnection = directConnection;
    }

    @Override
    public void open() throws Exception {
        if (this.connectionProvider != null) {
            this.connection = this.connectionProvider.createNewConnection();
        } else {
            this.connection = this.directConnection;
        }
        this.statement = (ClickHousePreparedStatement) this.connection.prepareStatement(this.sql);
    }

    @Override
    public void write(List<RowData> rows) throws Exception {
        this.statement.clearBatch();
        for (RowData rowData : rows) {
            this.converter.toClickHouse(rowData, this.statement);
            this.statement.addBatch();
        }
        this.statement.executeBatch();
        this.statement.clearBatch();
    }

    @Override
    public void reopen() throws Exception {
        close();
        open();
    }

    @Override
    public void close() throws Exception {
        if (this.statement != null) {
            try {
                this.statement.close();
            } finally {
                this.statement = null;
            }
        }
        if (this.connection != null && this.connection != this.directConnection) {
            try {
                this.connection.close();
            } finally {
                this.connection = null;
            }
        }
    }
}
