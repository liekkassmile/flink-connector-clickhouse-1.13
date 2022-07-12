package com.glab.flink.connector.clickhouse.table.internal.executor;

import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.except.ClickHouseException;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

public class ClickHouseBatchExecutor implements ClickHouseExecutor{
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);

    private transient ClickHousePreparedStatement stmt;

    private transient ClickHouseConnectionProvider connectionProvider;

    private RuntimeContext context;

    private TypeInformation<RowData> rowDataTypeInformation;

    private final String sql;

    private final ClickHouseRowConverter converter;

    private transient List<RowData> batch;

    private final Duration flushInterval;

    private final int maxRetries;

    private transient TypeSerializer<RowData> typeSerializer;

    private boolean objectReuseEnabled = false;

    private transient ClickHouseBatchExecutor.ExecuteBatchService service;

    public ClickHouseBatchExecutor(String sql,
                                   ClickHouseRowConverter converter,
                                   Duration flushInterval,
                                   int batchSize,
                                   int maxRetries,
                                   TypeInformation<RowData> rowDataTypeInformation) {
        this.sql = sql;
        this.converter = converter;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetries;
        this.rowDataTypeInformation = rowDataTypeInformation;
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.batch = new ArrayList<>();
        this.stmt = (ClickHousePreparedStatement)connection.prepareStatement(this.sql);

        if(this.service == null) {
            LOG.info("this.service is null,start create service.....");
            this.service = new ClickHouseBatchExecutor.ExecuteBatchService();
            if(!this.service.isRunning()) {
                LOG.info("this.service is stop,start service: " + this.service.serviceName());
                this.service.startAsync();
            }
        }
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException {
        this.batch = new ArrayList<>();
        this.connectionProvider = connectionProvider;
        this.stmt = (ClickHousePreparedStatement) connectionProvider.getConnection().prepareStatement(this.sql);

        if(this.service == null) {
            LOG.info("this.service is null,start create service.....");
            this.service = new ClickHouseBatchExecutor.ExecuteBatchService();
            if(!this.service.isRunning()) {
                LOG.info("this.service is stop,start service: " + this.service.serviceName());
                this.service.startAsync();
            }
        }
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        this.context = context;
        this.typeSerializer = this.rowDataTypeInformation.createSerializer(context.getExecutionConfig());
        this.objectReuseEnabled = context.getExecutionConfig().isObjectReuseEnabled();
    }

    @Override
    public synchronized void addBatch(RowData record) throws IOException{
        if(record.getRowKind() != RowKind.DELETE && record.getRowKind() != RowKind.UPDATE_BEFORE) {
            if(this.objectReuseEnabled) {
                this.batch.add(this.typeSerializer.copy(record));
            } else {
                this.batch.add(record);
            }
        }
    }

    @Override
    public synchronized void executeBatch() throws IOException {
        if(this.service.isRunning()) {
            this.notifyAll();
        } else {
            throw new IOException("executor unexpectedly terminated", this.service.failureCause());
        }
    }

    @Override
    public List<RowData> getBatch() {
        return this.batch;
    }

    private void attemptExecuteBatch() throws Exception{
        int i = 1;
        while(i < this.maxRetries) {
            try {
                this.stmt.executeBatch();
                this.stmt.clearBatch();
                this.batch.clear();
                break;
            }catch (SQLException e) {
                LOG.error("Clickhouse executeBatch error, retry times = {}", i , e);
                if(i >= this.maxRetries) {
                    throw new IOException(e);
                }

                try{
                    Thread.sleep((long) (1000 * i));
                }catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e1);
                }
                i++;
            }
        }
    }

    @Override
    public void closeStatement() throws SQLException {
        if(this.service != null) {
            this.service.stopAsync().awaitTerminated();
        } else {
            LOG.warn("executor closed before initialized");
        }

        if(this.stmt != null) {
            this.stmt.close();
            this.stmt = null;
        }
    }

    private class ExecuteBatchService extends AbstractExecutionThreadService{
        private ExecuteBatchService() {}

        @Override
        protected void run() throws Exception {
            while(this.isRunning()) {
                synchronized(ClickHouseBatchExecutor.this) {
                    ClickHouseBatchExecutor.this.wait(ClickHouseBatchExecutor.this.flushInterval.toMillis());
                    LOG.info("collect size:" + ClickHouseBatchExecutor.this.batch.size());

                    if(!ClickHouseBatchExecutor.this.batch.isEmpty()) {
                        for (RowData record : ClickHouseBatchExecutor.this.batch) {
                            ClickHouseBatchExecutor.this.converter.toClickHouse(record, ClickHouseBatchExecutor.this.stmt);
                            ClickHouseBatchExecutor.this.stmt.addBatch();
                        }
                        this.attemptExecuteBatch();
                    }
                }
            }
        }

        private void attemptExecuteBatch() throws Exception{
            for(int idx = 1; idx <= ClickHouseBatchExecutor.this.maxRetries; idx++) {
                try {
                    ClickHouseBatchExecutor.this.stmt.executeBatch();
                    ClickHouseBatchExecutor.this.batch.clear();
                    break;
                }catch (ClickHouseException e1) {
                    ClickHouseBatchExecutor.LOG.error("ClickHouse error", e1);
                    //当出现ClickHouse exception, code: 27 ...DB::Exception: Cannot parse input 即这条数据是错误时,略过此次插入
                    int errorCode = e1.getErrorCode();
                    if(errorCode == 27) {
                        batch.clear();
                        break;
                    }
                }catch (SQLException e2) {
                    ClickHouseBatchExecutor.LOG.error("ClickHouse executeBatch error, retry times = {}", Integer.valueOf(idx), e2);
                    if(idx >= ClickHouseBatchExecutor.this.maxRetries){
                        throw new IOException(e2);
                    }
                    try {
                        Thread.sleep((1000 * idx));
                    } catch (InterruptedException e3) {
                        Thread.currentThread().interrupt();
                        throw new IOException("unable to flush; interrupted while doing another attempt", e3);
                    }
                }
            }
        }

        @Override
        protected String serviceName() {
            return super.serviceName();
        }
    }
}
