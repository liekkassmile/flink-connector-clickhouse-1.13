package com.glab.flink.connector.clickhouse.table.internal;

import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.glab.flink.connector.clickhouse.table.internal.executor.ClickHouseBatchExecutor;
import com.glab.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import com.glab.flink.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseShardSinkFunction extends AbstractClickHouseSinkFunction{
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardSinkFunction.class);

    private static final Pattern PATTERN = Pattern.compile("Distributed\\((?<cluster>[a-zA-Z_'][0-9a-zA-Z_']*),\\s*(?<database>[a-zA-Z_'][0-9a-zA-Z_']*),\\s*(?<table>[a-zA-Z_'][0-9a-zA-Z_']*)");

    private final ClickHouseConnectionProvider connectionProvider;

    private final ClickHouseRowConverter converter;

    private final ClickHousePartitioner partitioner;

    private final ClickHouseOptions options;

    private final List<String> fieldNames;

    private final LogicalType[] logicalTypes;

    private transient boolean closed = false;

    private transient ClickHouseConnection connection;

    private String remoteTable;

    private String remoteDatabase;

    private transient List<ClickHouseConnection> shardConnections;

    private transient List<String> shardUrls;

    private final List<ClickHouseExecutor> shardExecutors;

    private final String[] keyFields;

    private final boolean ignoreDelete;

    private transient boolean objectReuseEnabled;

    protected ClickHouseShardSinkFunction(@Nonnull ClickHouseConnectionProvider connectionProvider,
                                          @Nonnull LogicalType[] logicalTypes,
                                          @Nonnull List<String> fieldNames,
                                          @Nonnull Optional<String[]> keyFields,
                                          @Nonnull ClickHouseRowConverter converter,
                                          @Nonnull ClickHousePartitioner partitioner,
                                          @Nonnull ClickHouseOptions options) {
        LOG.info("ClickHouseShardSinkFunction init ....");
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.logicalTypes = logicalTypes;
        this.fieldNames = Preconditions.checkNotNull(fieldNames);
        this.converter = Preconditions.checkNotNull(converter);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.options = Preconditions.checkNotNull(options);
        this.shardExecutors = new ArrayList<>();
        this.ignoreDelete = options.getIgnoreDelete();
        if (keyFields.isPresent()) {
            this.keyFields = keyFields.get();
        } else {
            this.keyFields = new String[0];
        }
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        try {
            this.connection = this.connectionProvider.getConnection();
            establishShardConnections();
            initializeExecutors();
            this.objectReuseEnabled = getRuntimeContext().getExecutionConfig().isObjectReuseEnabled();
        } catch (Exception e) {
            throw new IOException("unable to establish connection to ClickHouse", e);
        }
    }

    private void establishShardConnections() throws IOException {
        try {
            String engine = this.connectionProvider.queryTableEngine(this.options.getDatabaseName(), this.options.getTableName());
            Matcher matcher = PATTERN.matcher(engine);
            if (matcher.find()) {
                String remoteCluster = matcher.group("cluster").replace("'", "");
                this.remoteDatabase = matcher.group("database").replace("'", "");
                this.remoteTable = matcher.group("table").replace("'", "");
                this.shardConnections = this.connectionProvider.getShardConnections(remoteCluster, this.remoteDatabase);
                this.shardUrls = this.connectionProvider.getShardUrls();
            } else {
                throw new IOException("table `" + this.options.getDatabaseName() + "`.`" + this.options.getTableName() + "` is not a Distributed table");
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void initializeExecutors() throws SQLException {
        String sql = ClickHouseStatementFactory.getInsertIntoStatement(this.remoteTable, this.fieldNames);

        for (int i = 0; i < this.shardConnections.size(); i++) {
            ClickHouseExecutor executor;

            if (this.keyFields.length > 0) {
                executor = ClickHouseExecutor.createUpsertExecutor(this.remoteTable, this.fieldNames, this.keyFields, this.converter, this.options);
            } else {
                executor = new ClickHouseBatchExecutor(
                        sql,
                        this.converter,
                        this.options,
                        this.logicalTypes,
                        null,
                        this.fieldNames,
                        this.remoteDatabase,
                        this.remoteTable,
                        this.shardUrls != null && this.shardUrls.size() > i ? this.shardUrls.get(i) : null);
            }
            executor.prepareStatement(this.shardConnections.get(i));
            executor.setRuntimeContext(getRuntimeContext());
            this.shardExecutors.add(executor);
        }
    }

    @Override
    public void invoke(RowData record, Context context) throws Exception {
        switch (record.getRowKind()) {
            case INSERT:
                writeRecordToOneExecutor(record);
                break;
            case UPDATE_AFTER:
                if (this.ignoreDelete) {
                    writeRecordToOneExecutor(record);
                } else {
                    writeRecordToAllExecutors(record);
                }
                break;
            case DELETE:
                if (!this.ignoreDelete)
                    writeRecordToAllExecutors(record);
            case UPDATE_BEFORE:
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.", new Object[] { record.getRowKind() }));
        }
   }

    private void writeRecordToOneExecutor(RowData rowData) throws Exception {
        int selected = this.partitioner.select(rowData, this.shardExecutors.size());
        RowData record = genericRowData(rowData);
        this.shardExecutors.get(selected).addBatch(record);
    }

    private void writeRecordToAllExecutors(RowData record) throws IOException {
        RowData recordToWrite = genericRowData(record);
        for (int i = 0; i < this.shardExecutors.size(); ++i) {
            this.shardExecutors.get(i).addBatch(recordToWrite);
        }
    }

    /**
     * 生成一个新对象
     * @param record
     * @return
     * @throws Exception
     */
    public RowData genericRowData(RowData record){
        GenericRowData rowData = new GenericRowData(record.getArity());
        rowData.setRowKind(record.getRowKind());

        for(int i = 0; i < record.getArity(); i++) {
            if (record.isNullAt(i)) {
                rowData.setField(i, null);
                continue;
            }
            LogicalType fieldType = logicalTypes[i];
            switch(fieldType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    rowData.setField(i, record.getString(i));
                    break;
                case BOOLEAN:
                    rowData.setField(i, record.getBoolean(i));
                    break;
                case BINARY:
                case VARBINARY:
                    rowData.setField(i, record.getBinary(i));
                    break;
                case DECIMAL:
                    int decimalPrecision = LogicalTypeChecks.getPrecision(fieldType);
                    int decimalScale = LogicalTypeChecks.getScale(fieldType);
                    rowData.setField(i, record.getDecimal(i, decimalPrecision, decimalScale));
                    break;
                case TINYINT:
                    rowData.setField(i, record.getByte(i));
                    break;
                case SMALLINT:
                    rowData.setField(i, record.getShort(i));
                    break;
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case INTERVAL_YEAR_MONTH:
                    rowData.setField(i, record.getInt(i));
                    break;
                case BIGINT:
                case INTERVAL_DAY_TIME:
                    rowData.setField(i, record.getLong(i));
                    break;
                case FLOAT:
                    rowData.setField(i, record.getFloat(i));
                    break;
                case DOUBLE:
                    rowData.setField(i, record.getDouble(i));
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    int timestampPrecision = LogicalTypeChecks.getPrecision(fieldType);
                    rowData.setField(i, record.getTimestamp(i, timestampPrecision));
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    throw new UnsupportedOperationException();
                case ARRAY:
                    rowData.setField(i, record.getArray(i));
                    break;
                case MULTISET:
                case MAP:
                    rowData.setField(i, record.getMap(i));
                    break;
                case ROW:
                case STRUCTURED_TYPE:
                    int rowFieldCount = LogicalTypeChecks.getFieldCount(fieldType);
                    rowData.setField(i, record.getRow(i, rowFieldCount));
                    break;
                case DISTINCT_TYPE:
                    break;
                case RAW:
                    rowData.setField(i, record.getRawValue(i));
                    break;
                case NULL:
                case SYMBOL:
                case UNRESOLVED:
                default:
                    throw new IllegalArgumentException();
            }
            //bwRowdata.setNonPrimitiveValue(i, record.getString(i));
        }
        return rowData;
    }

    @Override
    public void flush() throws IOException {
        for (int i = 0; i < this.shardExecutors.size(); ++i) {
            this.flush(i);
        }
    }

    public void flush(int index) throws IOException {
        this.shardExecutors.get(index).executeBatch();
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            try {
                this.flush();
            } catch (Exception e) {
                LOG.warn("Writing records to ClickHouse failed.", e);
            }
            this.closeConnection();
        }
    }

    private void closeConnection() {
        if (this.connection != null)
            try {
                for (int i = 0; i < this.shardExecutors.size(); ++i){
                    this.shardExecutors.get(i).closeStatement();
                }
                this.connectionProvider.closeConnection();
            } catch (SQLException se) {
                LOG.warn("ClickHouse connection could not be closed: {}", se.getMessage());
            } finally {
                this.connection = null;
            }
    }
}
