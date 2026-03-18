package com.glab.flink.connector.clickhouse.table.internal.options;

import com.glab.flink.connector.clickhouse.table.internal.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

public class ClickHouseOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String url;

    @Nullable
    private final String username;

    @Nullable
    private final String password;

    private final String databaseName;

    private final String tableName;

    private final int batchSize;

    private final long batchBytes;

    private final Duration flushInterval;

    private final int maxRetries;

    private final int maxBufferedRows;

    private final int maxInFlightBatches;

    private final int flushThreadNum;

    private final boolean preferLargeBatch;

    private final int partialFlushMinRows;

    private final String writerType;

    private final Duration httpConnectTimeout;

    private final Duration httpSocketTimeout;

    private final boolean writeLocal;

    private final String partitionStrategy;

    private final String partitionKey;

    private final boolean ignoreDelete;

    private final String sinkMode;

    private ClickHouseDialect dialect;

    private ClickHouseOptions(String url, String username, String password, String databaseName, String tableName, int batchSize, long batchBytes, Duration flushInterval, int maxRetires, int maxBufferedRows, int maxInFlightBatches, int flushThreadNum, boolean preferLargeBatch, int partialFlushMinRows, String writerType, Duration httpConnectTimeout, Duration httpSocketTimeout, boolean writeLocal, String partitionStrategy, String partitionKey, boolean ignoreDelete, String sinkMode, ClickHouseDialect dialect) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.batchBytes = batchBytes;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetires;
        this.maxBufferedRows = maxBufferedRows;
        this.maxInFlightBatches = maxInFlightBatches;
        this.flushThreadNum = flushThreadNum;
        this.preferLargeBatch = preferLargeBatch;
        this.partialFlushMinRows = partialFlushMinRows;
        this.writerType = writerType;
        this.httpConnectTimeout = httpConnectTimeout;
        this.httpSocketTimeout = httpSocketTimeout;
        this.writeLocal = writeLocal;
        this.partitionStrategy = partitionStrategy;
        this.partitionKey = partitionKey;
        this.ignoreDelete = ignoreDelete;
        this.sinkMode = sinkMode;
        this.dialect = dialect;
    }

    public String getUrl() {
        return this.url;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(this.username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(this.password);
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    public long getBatchBytes() {
        return this.batchBytes;
    }

    public Duration getFlushInterval() {
        return this.flushInterval;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    public int getMaxBufferedRows() {
        return this.maxBufferedRows;
    }

    public int getMaxInFlightBatches() {
        return this.maxInFlightBatches;
    }

    public int getFlushThreadNum() {
        return this.flushThreadNum;
    }

    public boolean getPreferLargeBatch() {
        return this.preferLargeBatch;
    }

    public int getPartialFlushMinRows() {
        return this.partialFlushMinRows;
    }

    public boolean getWriteLocal() {
        return this.writeLocal;
    }

    public String getWriterType() {
        return this.writerType;
    }

    public Duration getHttpConnectTimeout() {
        return this.httpConnectTimeout;
    }

    public Duration getHttpSocketTimeout() {
        return this.httpSocketTimeout;
    }

    public String getPartitionStrategy() {
        return this.partitionStrategy;
    }

    public String getPartitionKey() {
        return this.partitionKey;
    }

    public boolean getIgnoreDelete() {
        return this.ignoreDelete;
    }

    public String getSinkMode() {
        return this.sinkMode;
    }

    public ClickHouseDialect getDialect() {return this.dialect; }

    public static class Builder {
        private String url;

        private String username;

        private String password;

        private String databaseName;

        private String tableName;

        private int batchSize;

        private long batchBytes;

        //flush 时间间隔
        private Duration flushInterval;

        //最大重试次数
        private int maxRetries;

        private int maxBufferedRows;

        private int maxInFlightBatches;

        private int flushThreadNum;

        private boolean preferLargeBatch;

        private int partialFlushMinRows;

        private String writerType;

        private Duration httpConnectTimeout;

        private Duration httpSocketTimeout;

        //是否写本地表
        private boolean writeLocal;

        //分区策略hash | random | balanced
        private String partitionStrategy;

        //hash 策略下的分区键
        private String partitionKey;

        //忽略 DELETE 并视 UPDATE 为 INSERT
        private boolean ignoreDelete;

        private String sinkMode;

        private ClickHouseDialect dialect;

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withBatchBytes(long batchBytes) {
            this.batchBytes = batchBytes;
            return this;
        }

        public Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder withMaxBufferedRows(int maxBufferedRows) {
            this.maxBufferedRows = maxBufferedRows;
            return this;
        }

        public Builder withMaxInFlightBatches(int maxInFlightBatches) {
            this.maxInFlightBatches = maxInFlightBatches;
            return this;
        }

        public Builder withFlushThreadNum(int flushThreadNum) {
            this.flushThreadNum = flushThreadNum;
            return this;
        }

        public Builder withPreferLargeBatch(boolean preferLargeBatch) {
            this.preferLargeBatch = preferLargeBatch;
            return this;
        }

        public Builder withPartialFlushMinRows(int partialFlushMinRows) {
            this.partialFlushMinRows = partialFlushMinRows;
            return this;
        }

        public Builder withWriterType(String writerType) {
            this.writerType = writerType;
            return this;
        }

        public Builder withHttpConnectTimeout(Duration httpConnectTimeout) {
            this.httpConnectTimeout = httpConnectTimeout;
            return this;
        }

        public Builder withHttpSocketTimeout(Duration httpSocketTimeout) {
            this.httpSocketTimeout = httpSocketTimeout;
            return this;
        }

        public Builder withWriteLocal(Boolean writeLocal) {
            this.writeLocal = writeLocal.booleanValue();
            return this;
        }

        public Builder withPartitionStrategy(String partitionStrategy) {
            this.partitionStrategy = partitionStrategy;
            return this;
        }

        public Builder withPartitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public Builder withIgnoreDelete(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
            return this;
        }

        public Builder withSinkMode(String sinkMode) {
            this.sinkMode = sinkMode;
            return this;
        }

        public ClickHouseOptions.Builder setDialect(ClickHouseDialect dialect) {
            this.dialect = dialect;
            return this;
        }

        public ClickHouseOptions build() {
            Preconditions.checkNotNull(this.url, "No dbURL supplied.");
            Preconditions.checkNotNull(this.tableName, "No tableName supplied.");
            Optional optional;
            if (this.dialect == null) {
                optional = JdbcDialects.get(this.url);
                this.dialect = (ClickHouseDialect) optional.orElseGet(() -> {
                    throw new NullPointerException("Unknown dbURL,can not find proper dialect.");
                });
            }
            return new ClickHouseOptions(this.url, this.username, this.password, this.databaseName, this.tableName, this.batchSize, this.batchBytes, this.flushInterval, this.maxRetries, this.maxBufferedRows, this.maxInFlightBatches, this.flushThreadNum, this.preferLargeBatch, this.partialFlushMinRows, this.writerType, this.httpConnectTimeout, this.httpSocketTimeout, this.writeLocal, this.partitionStrategy, this.partitionKey, this.ignoreDelete, this.sinkMode, this.dialect);
        }
    }
}
