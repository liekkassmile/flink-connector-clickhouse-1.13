package com.glab.flink.connector.clickhouse.table.internal.options;

import com.glab.flink.connector.clickhouse.table.internal.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
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

    private final Duration flushInterval;

    private final int maxRetries;

    private final boolean writeLocal;

    private final String partitionStrategy;

    private final String partitionKey;

    private final boolean ignoreDelete;

    private ClickHouseDialect dialect;

    private ClickHouseOptions(String url, String username, String password, String databaseName, String tableName, int batchSize, Duration flushInterval, int maxRetires, boolean writeLocal, String partitionStrategy, String partitionKey, boolean ignoreDelete, ClickHouseDialect dialect) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetires;
        this.writeLocal = writeLocal;
        this.partitionStrategy = partitionStrategy;
        this.partitionKey = partitionKey;
        this.ignoreDelete = ignoreDelete;
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

    public Duration getFlushInterval() {
        return this.flushInterval;
    }

    public int getMaxRetries() {
        return this.maxRetries;
    }

    public boolean getWriteLocal() {
        return this.writeLocal;
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

    public ClickHouseDialect getDialect() {return this.dialect; }

    public static class Builder {
        private String url;

        private String username;

        private String password;

        private String databaseName;

        private String tableName;

        private int batchSize;

        //flush 时间间隔
        private Duration flushInterval;

        //最大重试次数
        private int maxRetries;

        //是否写本地表
        private boolean writeLocal;

        //分区策略hash | random | balanced
        private String partitionStrategy;

        //hash 策略下的分区键
        private String partitionKey;

        //忽略 DELETE 并视 UPDATE 为 INSERT
        private boolean ignoreDelete;

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

        public Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder withMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
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
            return new ClickHouseOptions(this.url, this.username, this.password, this.databaseName, this.tableName, this.batchSize, this.flushInterval, this.maxRetries, this.writeLocal, this.partitionStrategy, this.partitionKey, this.ignoreDelete, this.dialect);
        }
    }
}
