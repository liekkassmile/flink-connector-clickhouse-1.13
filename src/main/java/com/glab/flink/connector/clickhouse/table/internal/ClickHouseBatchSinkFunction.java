package com.glab.flink.connector.clickhouse.table.internal;

import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import javax.annotation.Nonnull;
import java.io.IOException;

public class ClickHouseBatchSinkFunction extends AbstractClickHouseSinkFunction{
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchSinkFunction.class);

    private final ClickHouseConnectionProvider connectionProvider;

    private transient ClickHouseConnection connection;

    private final ClickHouseExecutor executor;

    private final ClickHouseOptions options;

    private transient boolean closed = false;

    private transient int batchCount = 0;

    public ClickHouseBatchSinkFunction(@Nonnull ClickHouseConnectionProvider connectionProvider,
                                       @Nonnull ClickHouseExecutor executor,
                                       @Nonnull ClickHouseOptions options) {
        this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
        this.executor = Preconditions.checkNotNull(executor);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        try {
           this.connection = this.connectionProvider.getConnection();
           this.executor.prepareStatement(this.connectionProvider);
           this.executor.setRuntimeContext(getRuntimeContext());
        }catch (Exception e) {
            throw new IOException("unable to establish connection with ClickHouse", e);
        }
    }

    @Override
    public void invoke(RowData rowData, Context context) throws IOException{
        this.executor.addBatch(rowData);
        this.batchCount++;
        if(this.batchCount >= this.options.getBatchSize()) {
            LOG.info("flush :" + this.batchCount + "条数据!!");
            flush();
            this.batchCount = 0;
        }
    }

    @Override
    public void flush() throws IOException {
        this.executor.executeBatch();
    }

    @Override
    public void close() {
        if(!this.closed) {
            this.closed = true;
            try {
                flush();
            } catch (Exception e) {
                LOG.warn("Writing records to ClickHouse failed.", e);
            }

            closeConnection();
        }
    }

    private void closeConnection(){
        if(this.connection != null) {
            try {
                this.executor.closeStatement();
                this.connectionProvider.closeConnection();
            }catch (Exception e) {
                LOG.warn("ClickHouse connection could not be closed: {}", e.getMessage());
            } finally {
                this.connection = null;
            }
        }
    }

}
