package com.glab.flink.connector.clickhouse.table.internal.executor;

import org.apache.flink.table.data.RowData;

import java.util.List;

public interface ClickHouseBulkWriter {
    void open() throws Exception;

    void write(List<RowData> rows) throws Exception;

    // rowbinary 路径优先走预编码批次，JDBC 路径仍保留旧的按 RowData 写入方式。
    default void write(EncodedBatch batch) throws Exception {
        throw new UnsupportedOperationException("encoded batch write is not supported");
    }

    void reopen() throws Exception;

    void close() throws Exception;
}
