package com.glab.flink.connector.clickhouse.table.internal;

import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.glab.flink.connector.clickhouse.table.internal.dialect.ClickHouseDialect;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author lrh
 * @date 2021/6/22
 */
public class ClickHouseRowDataLookupFunction extends TableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final String query;
    private final ClickHouseConnectionProvider connectionProvider;
    private final DataType[] keyTypes;
    private final String[] keyNames;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTiems;
    private final ClickHouseDialect dialect;
    private final ClickHouseRowConverter rowConverter;
    private final ClickHouseRowConverter lookupJdbcRowConverter;
    private transient ClickHouseConnection connection;

    private transient ClickHousePreparedStatement statement;
    private transient Cache<RowData, List<RowData>> cache;

    public ClickHouseRowDataLookupFunction(ClickHouseOptions options,
                                           JdbcLookupOptions lookupOptions,
                                           String[] fieldNames,
                                           DataType[] fieldTypes,
                                           String[] keyNames,
                                           RowType rowType) {

        Preconditions.checkNotNull(options, "No JdbcOptions supplied.");
        Preconditions.checkNotNull(fieldNames, "No fieldNames supplied.");
        Preconditions.checkNotNull(fieldTypes, "No fielsTypes supplied.");
        Preconditions.checkNotNull(keyNames, "No keyNames supplied.");

        this.connectionProvider = new ClickHouseConnectionProvider(options);
        this.keyNames = keyNames;
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes = Arrays.stream(keyNames).map(s ->
        {Preconditions.checkArgument(nameList.contains(s),
                "keyName %s cant find in fieldNams %s."
                                    ,s
                                    ,nameList);
        return fieldTypes[nameList.indexOf(s)];
        }).toArray(DataType[]::new);

        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTiems = lookupOptions.getMaxRetryTimes();

        this.query = options.getDialect().getSelectFromStatement(options.getTableName(), fieldNames, keyNames);

        String url = options.getUrl();
        this.dialect = new ClickHouseDialect();
        this.rowConverter = (ClickHouseRowConverter)dialect.getRowConverter(rowType);
        this.lookupJdbcRowConverter = (ClickHouseRowConverter)dialect.getRowConverter(
                RowType.of(Arrays.stream(keyTypes)
                        .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new)));

    }


    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            this.connection = connectionProvider.getConnection();
            this.statement = (ClickHousePreparedStatement) connection.prepareStatement(query, keyNames);

            this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null :
                    CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
        } catch (SQLException e1) {
            throw new IllegalArgumentException("open() failed", e1);
        }
    }

    public void eval(Object...keys) {
        RowData keyRow = GenericRowData.of(keys);

        if(cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if(cachedRows != null) {
                for (RowData cachedRow : cachedRows) {
                    collect(cachedRow);
                }
                return;
            }
        }

        for(int retry = 0; retry <= maxRetryTiems; retry++) {
            try {
                statement.clearParameters();
                statement  = lookupJdbcRowConverter.toClickHouse(keyRow, statement);

                ResultSet resultSet = statement.executeQuery();
                if(cache == null) {
                    while(resultSet.next()) {
                        collect(rowConverter.toInternal(resultSet));
                    }
                } else {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while(resultSet.next()) {
                        RowData row = rowConverter.toInternal(resultSet);
                        rows.add(row);
                        collect(row);
                    }

                    rows.trimToSize();
                    cache.put(keyRow, rows);
                }

                break;
            }catch (SQLException e1) {
                LOG.error(String.format("JDBC executionBatch error, retry times = %d", retry), e1);

                if(retry > maxRetryTiems) {
                    throw new RuntimeException("Execution of JDBC statement failed." , e1);
                }

                try {
                    if(connection != null) {
                        statement.close();
                        connectionProvider.closeConnection();
                        this.connection = connectionProvider.getConnection();
                        this.statement = (ClickHousePreparedStatement) this.connection.prepareStatement(query, keyNames);
                    }
                }catch (SQLException e2) {
                    LOG.error("JDBC connection is not valid, and reestablish connection failed.");
                    throw new RuntimeException("Reestablish JDBC connection failed", e2);
                }

                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e3) {
                    throw new RuntimeException(e3);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if(cache != null) {
            cache.cleanUp();
            cache = null;
        }

        if(statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("JDBC statement could not be closed." + e.getMessage());
            } finally {
                statement = null;
            }
        }

        connectionProvider.closeConnection();
    }

    @VisibleForTesting
    public Connection getDbConnection() throws SQLException{
        return connectionProvider.getConnection();
    }
}
