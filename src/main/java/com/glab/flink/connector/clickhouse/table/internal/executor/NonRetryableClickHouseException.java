package com.glab.flink.connector.clickhouse.table.internal.executor;

import java.io.IOException;

public class NonRetryableClickHouseException extends IOException {
    public NonRetryableClickHouseException(String message) {
        super(message);
    }
}
