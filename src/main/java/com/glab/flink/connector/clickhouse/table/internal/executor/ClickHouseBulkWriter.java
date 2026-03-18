package com.glab.flink.connector.clickhouse.table.internal.executor;

import org.apache.flink.table.data.RowData;

import java.util.List;

public interface ClickHouseBulkWriter {
    void open() throws Exception;

    void write(List<RowData> rows) throws Exception;

    void reopen() throws Exception;

    void close() throws Exception;
}
