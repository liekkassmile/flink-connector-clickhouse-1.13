package com.glab.flink.connector.clickhouse.table.internal.executor;

import com.glab.flink.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowBinaryEncoder;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ClickHouseBatchExecutor implements ClickHouseExecutor {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);

    private final ClickHouseOptions options;
    private final String sql;
    private final ClickHouseRowConverter converter;
    private final Duration flushInterval;
    private final int maxRetries;
    private final int batchSize;
    private final long batchBytes;
    private final int maxBufferedRows;
    private final int maxInFlightBatches;
    private final int flushThreadNum;
    private final boolean preferLargeBatch;
    private final int partialFlushMinRows;
    private final TypeInformation<RowData> rowDataTypeInformation;
    private final LogicalType[] logicalTypes;
    private final List<String> fieldNames;
    private final String targetDatabaseName;
    private final String targetTableName;
    private final String targetUrl;
    private final BufferMemoryLimiter sharedMemoryLimiter;
    private final boolean useEncodedBuffer;

    private transient ClickHouseConnectionProvider connectionProvider;
    private transient ClickHouseConnection directConnection;
    private transient TypeSerializer<RowData> typeSerializer;
    private transient final Object stateLock = new Object();
    private transient BatchBuffer activeBuffer;
    private transient int bufferedRows;
    private transient long bufferedBytes;
    private transient int inFlightBuffers;
    private transient BlockingQueue<BatchBuffer> flushQueue;
    private transient ScheduledExecutorService flushScheduler;
    private transient List<FlushWorker> flushWorkers;
    private transient long lastFlushRows;
    private transient long lastFlushBytes;
    private transient long lastFlushLatencyMs;
    private transient Counter flushCounter;
    private transient Counter retryCounter;
    private transient Counter errorCounter;
    private transient Counter smallBatchFlushCounter;
    private transient ClickHouseRowBinaryEncoder rowBinaryEncoder;
    private transient DynamicByteArrayOutputStream rowEncodingBuffer;
    private transient volatile boolean running;
    private transient volatile Throwable failure;

    public ClickHouseBatchExecutor(String sql,
                                   ClickHouseRowConverter converter,
                                   ClickHouseOptions options,
                                   LogicalType[] logicalTypes,
                                   TypeInformation<RowData> rowDataTypeInformation,
                                   List<String> fieldNames,
                                   String targetDatabaseName,
                                   String targetTableName,
                                   String targetUrl) {
        this(sql, converter, options, logicalTypes, rowDataTypeInformation, fieldNames, targetDatabaseName, targetTableName, targetUrl, null);
    }

    public ClickHouseBatchExecutor(String sql,
                                   ClickHouseRowConverter converter,
                                   ClickHouseOptions options,
                                   LogicalType[] logicalTypes,
                                   TypeInformation<RowData> rowDataTypeInformation,
                                   List<String> fieldNames,
                                   String targetDatabaseName,
                                   String targetTableName,
                                   String targetUrl,
                                   BufferMemoryLimiter sharedMemoryLimiter) {
        this.options = options;
        this.sql = sql;
        this.converter = converter;
        this.flushInterval = options.getFlushInterval();
        this.maxRetries = options.getMaxRetries();
        this.batchSize = options.getBatchSize();
        this.batchBytes = options.getBatchBytes();
        this.maxBufferedRows = Math.max(options.getMaxBufferedRows(), options.getBatchSize());
        this.maxInFlightBatches = Math.max(1, options.getMaxInFlightBatches());
        this.flushThreadNum = Math.max(1, options.getFlushThreadNum());
        this.preferLargeBatch = options.getPreferLargeBatch();
        this.partialFlushMinRows = Math.max(0, options.getPartialFlushMinRows());
        this.rowDataTypeInformation = rowDataTypeInformation;
        this.logicalTypes = logicalTypes;
        this.fieldNames = fieldNames;
        this.targetDatabaseName = targetDatabaseName;
        this.targetTableName = targetTableName;
        this.targetUrl = targetUrl;
        this.sharedMemoryLimiter = sharedMemoryLimiter;
        this.useEncodedBuffer = "http-rowbinary".equals(options.getWriterType());
    }

    @Override
    public void prepareStatement(ClickHouseConnection connection) throws SQLException {
        this.directConnection = connection;
        this.connectionProvider = null;
        initializeEncodingIfNeeded();
        initializeRuntime(1);
    }

    @Override
    public void prepareStatement(ClickHouseConnectionProvider connectionProvider) throws SQLException {
        this.connectionProvider = connectionProvider;
        this.directConnection = null;
        initializeEncodingIfNeeded();
        initializeRuntime(this.flushThreadNum);
    }

    private void initializeEncodingIfNeeded() throws SQLException {
        if (!this.useEncodedBuffer) {
            this.rowBinaryEncoder = null;
            this.rowEncodingBuffer = null;
            return;
        }
        try {
            // http-rowbinary 模式下在 open 阶段初始化编码器，后续每条数据进入缓冲前就完成编码。
            List<String> clickHouseTypes = HttpRowBinaryWriter.loadClickHouseTypes(
                    this.options,
                    this.targetUrl == null ? this.options.getUrl() : this.targetUrl,
                    this.targetDatabaseName,
                    this.targetTableName,
                    this.fieldNames);
            this.rowBinaryEncoder = new ClickHouseRowBinaryEncoder(this.logicalTypes, clickHouseTypes);
            int initialCapacity = (int) Math.max(256L, Math.min(this.batchBytes > 0 ? this.batchBytes / 8L : 4096L, 1L << 20));
            this.rowEncodingBuffer = new DynamicByteArrayOutputStream(initialCapacity);
        } catch (Exception e) {
            throw new SQLException("failed to initialize rowbinary encoder", e);
        }
    }

    private void initializeRuntime(int workerCount) {
        this.flushQueue = new ArrayBlockingQueue<>(this.maxInFlightBatches);
        this.activeBuffer = new BatchBuffer(System.nanoTime());
        this.bufferedRows = 0;
        this.bufferedBytes = 0L;
        this.inFlightBuffers = 0;
        this.failure = null;
        this.running = true;
        this.flushWorkers = new ArrayList<>();

        for (int i = 0; i < workerCount; i++) {
            FlushWorker worker = new FlushWorker(i);
            worker.start();
            this.flushWorkers.add(worker);
        }

        this.flushScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ck-batch-flush-timer"));
        long flushMillis = Math.max(1L, this.flushInterval.toMillis());
        this.flushScheduler.scheduleWithFixedDelay(() -> {
            try {
                triggerTimeBasedFlush();
            } catch (Throwable t) {
                registerFailure(t);
            }
        }, flushMillis, flushMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setRuntimeContext(RuntimeContext context) {
        if (this.rowDataTypeInformation != null) {
            this.typeSerializer = this.rowDataTypeInformation.createSerializer(context.getExecutionConfig());
        }
        registerMetrics(context);
    }

    @Override
    public void addBatch(RowData record) throws IOException {
        if (record.getRowKind() == RowKind.DELETE || record.getRowKind() == RowKind.UPDATE_BEFORE) {
            return;
        }

        ensureNoFailure();

        RowData rowToStore = copyIfNeeded(record);
        EncodedBatch encodedRow = null;
        long bytesToBuffer;
        if (this.useEncodedBuffer) {
            // 先编码、再入缓冲，这样 batch-bytes 统计的是实际发送字节而不是估算值。
            encodedRow = encodeRow(rowToStore);
            bytesToBuffer = encodedRow.size();
            if (this.sharedMemoryLimiter != null) {
                this.sharedMemoryLimiter.acquire(bytesToBuffer);
            }
        } else {
            bytesToBuffer = estimateRowSize(rowToStore);
        }

        BatchBuffer bufferToFlush = null;
        boolean added = false;
        synchronized (this.stateLock) {
            try {
                waitForBufferCapacity();
                ensureNoFailure();
                if (!this.running) {
                    throw new IOException("executor already closed");
                }

                this.activeBuffer.add(rowToStore, encodedRow, bytesToBuffer);
                added = true;
            } finally {
                if (!added && this.sharedMemoryLimiter != null && encodedRow != null) {
                    this.sharedMemoryLimiter.release(bytesToBuffer);
                }
            }
            this.bufferedRows++;
            this.bufferedBytes += bytesToBuffer;
            if (this.activeBuffer.shouldFlush()) {
                bufferToFlush = rotateActiveBuffer();
            }
        }

        enqueueBuffer(bufferToFlush);
    }

    @Override
    public void executeBatch() throws IOException {
        ensureNoFailure();
        enqueueBuffer(rotateIfNotEmpty(true));
    }

    @Override
    public void closeStatement() throws SQLException {
        try {
            executeBatch();
            awaitDrained();
        } catch (IOException e) {
            throw new SQLException("failed to flush buffered records before closing", e);
        } finally {
            this.running = false;
            if (this.flushScheduler != null) {
                this.flushScheduler.shutdownNow();
            }
            if (this.flushWorkers != null) {
                for (FlushWorker worker : this.flushWorkers) {
                    worker.interrupt();
                }
                for (FlushWorker worker : this.flushWorkers) {
                    try {
                        worker.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("interrupted while waiting flush workers to stop", e);
                    }
                }
            }
        }
    }

    @Override
    public List<RowData> getBatch() {
        synchronized (this.stateLock) {
            if (this.activeBuffer == null || this.activeBuffer.rows == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(this.activeBuffer.rows);
        }
    }

    private void triggerTimeBasedFlush() throws IOException {
        enqueueBuffer(rotateIfTimedOut());
    }

    private BatchBuffer rotateIfTimedOut() throws IOException {
        synchronized (this.stateLock) {
            if (this.activeBuffer == null || this.activeBuffer.isEmpty()) {
                return null;
            }

            long now = System.nanoTime();
            long ageNanos = now - this.activeBuffer.createdNanos;
            long flushNanos = this.flushInterval.toNanos();
            long graceNanos = flushNanos / 5;
            if (ageNanos < flushNanos) {
                return null;
            }
            if (this.preferLargeBatch
                    && this.activeBuffer.rowCount < this.partialFlushMinRows
                    && ageNanos < flushNanos + graceNanos) {
                return null;
            }
            if (this.smallBatchFlushCounter != null && this.activeBuffer.rowCount < this.partialFlushMinRows) {
                this.smallBatchFlushCounter.inc();
            }
            return rotateActiveBuffer();
        }
    }

    private BatchBuffer rotateIfNotEmpty(boolean force) {
        synchronized (this.stateLock) {
            if (this.activeBuffer == null || this.activeBuffer.isEmpty()) {
                return null;
            }
            if (!force && !this.activeBuffer.shouldFlush()) {
                return null;
            }
            return rotateActiveBuffer();
        }
    }

    private BatchBuffer rotateActiveBuffer() {
        BatchBuffer sealed = this.activeBuffer;
        // 切换活跃缓冲时把流式拼接结果封口，生成可重试、可重复发送的整批字节块。
        sealed.seal();
        this.activeBuffer = new BatchBuffer(System.nanoTime());
        this.inFlightBuffers++;
        return sealed;
    }

    private void enqueueBuffer(BatchBuffer batchBuffer) throws IOException {
        if (batchBuffer == null || batchBuffer.isEmpty()) {
            return;
        }

        while (this.running) {
            ensureNoFailure();
            try {
                if (this.flushQueue.offer(batchBuffer, 200L, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for a flush slot", e);
            }
        }
        ensureNoFailure();
        throw new IOException("executor already closed");
    }

    private void awaitDrained() throws IOException {
        synchronized (this.stateLock) {
            while (this.running && (this.bufferedRows > 0 || this.inFlightBuffers > 0 || (this.activeBuffer != null && !this.activeBuffer.isEmpty()))) {
                ensureNoFailure();
                try {
                    this.stateLock.wait(200L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while waiting buffered data to flush", e);
                }
            }
            ensureNoFailure();
        }
    }

    private void waitForBufferCapacity() throws IOException {
        while (this.bufferedRows >= this.maxBufferedRows) {
            ensureNoFailure();
            try {
                this.stateLock.wait(200L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for available buffer capacity", e);
            }
        }
    }

    private RowData copyIfNeeded(RowData record) {
        if (this.typeSerializer != null) {
            return this.typeSerializer.copy(record);
        }
        return record;
    }

    private EncodedBatch encodeRow(RowData rowData) throws IOException {
        if (this.rowBinaryEncoder == null || this.rowEncodingBuffer == null) {
            throw new IOException("rowbinary encoder is not initialized");
        }
        // 单行编码缓冲重复利用，避免每条数据都新建 ByteArrayOutputStream。
        this.rowEncodingBuffer.reset();
        this.rowBinaryEncoder.encodeRow(rowData, this.rowEncodingBuffer);
        return this.rowEncodingBuffer.toEncodedBatch();
    }

    private void ensureNoFailure() throws IOException {
        if (this.failure != null) {
            throw new IOException("batch executor failed", this.failure);
        }
    }

    private void registerFailure(Throwable throwable) {
        LOG.error("ClickHouse batch executor failed", throwable);
        if (this.errorCounter != null) {
            this.errorCounter.inc();
        }
        this.failure = throwable;
        this.running = false;
        synchronized (this.stateLock) {
            this.stateLock.notifyAll();
        }
    }

    private void registerMetrics(RuntimeContext context) {
        try {
            // Access getMetricGroup via reflection to avoid return-type linkage issues
            // between different Flink minor versions such as 1.13 and 1.14.
            Method metricGroupMethod = context.getClass().getMethod("getMetricGroup");
            Object metricGroupObject = metricGroupMethod.invoke(context);
            if (!(metricGroupObject instanceof MetricGroup)) {
                LOG.warn("Skip ClickHouse sink metrics registration because RuntimeContext metric group is not compatible: {}",
                        metricGroupObject == null ? "null" : metricGroupObject.getClass().getName());
                return;
            }
            MetricGroup metricGroup = ((MetricGroup) metricGroupObject).addGroup("clickhouseSink");
            this.flushCounter = metricGroup.counter("flushCount");
            this.retryCounter = metricGroup.counter("retryCount");
            this.errorCounter = metricGroup.counter("writeErrorCount");
            this.smallBatchFlushCounter = metricGroup.counter("smallBatchFlushCount");
            metricGroup.gauge("pendingRows", () -> this.bufferedRows);
            metricGroup.gauge("pendingBytes", () -> this.bufferedBytes);
            metricGroup.gauge("inFlightBatches", () -> this.inFlightBuffers);
            metricGroup.gauge("lastFlushRows", () -> this.lastFlushRows);
            metricGroup.gauge("lastFlushBytes", () -> this.lastFlushBytes);
            metricGroup.gauge("lastFlushLatencyMs", () -> this.lastFlushLatencyMs);
        } catch (Throwable t) {
            LOG.warn("Skip ClickHouse sink metrics registration because RuntimeContext metrics API is not compatible", t);
        }
    }

    private long estimateRowSize(RowData rowData) {
        if (this.logicalTypes == null) {
            return Math.max(64L, rowData.getArity() * 16L);
        }

        long estimatedBytes = 16L;
        int arity = Math.min(rowData.getArity(), this.logicalTypes.length);
        for (int i = 0; i < arity; i++) {
            if (rowData.isNullAt(i)) {
                estimatedBytes += 1L;
                continue;
            }

            LogicalTypeRoot typeRoot = this.logicalTypes[i].getTypeRoot();
            switch (typeRoot) {
                case BOOLEAN:
                case TINYINT:
                    estimatedBytes += 1L;
                    break;
                case SMALLINT:
                    estimatedBytes += 2L;
                    break;
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                case INTERVAL_YEAR_MONTH:
                case FLOAT:
                    estimatedBytes += 4L;
                    break;
                case BIGINT:
                case DOUBLE:
                case INTERVAL_DAY_TIME:
                    estimatedBytes += 8L;
                    break;
                case CHAR:
                case VARCHAR:
                    estimatedBytes += estimateStringBytes(rowData.getString(i));
                    break;
                case BINARY:
                case VARBINARY:
                    estimatedBytes += rowData.getBinary(i).length;
                    break;
                case DECIMAL:
                    estimatedBytes += estimateDecimalBytes(rowData.getDecimal(i, 38, 18));
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                    estimatedBytes += estimateTimestampBytes(rowData.getTimestamp(i, 3));
                    break;
                case ARRAY:
                    estimatedBytes += estimateArrayBytes(rowData.getArray(i));
                    break;
                case MAP:
                case MULTISET:
                    estimatedBytes += estimateMapBytes(rowData.getMap(i));
                    break;
                case ROW:
                case STRUCTURED_TYPE:
                    estimatedBytes += 64L;
                    break;
                case RAW:
                    estimatedBytes += estimateRawBytes(rowData.getRawValue(i));
                    break;
                default:
                    estimatedBytes += 16L;
            }
        }
        return Math.max(estimatedBytes, 32L);
    }

    private long estimateStringBytes(StringData stringData) {
        return stringData == null ? 0L : stringData.toBytes().length;
    }

    private long estimateDecimalBytes(DecimalData decimalData) {
        return decimalData == null ? 0L : Math.max(16L, decimalData.toBigDecimal().precision());
    }

    private long estimateTimestampBytes(TimestampData timestampData) {
        return timestampData == null ? 0L : 12L;
    }

    private long estimateArrayBytes(ArrayData arrayData) {
        return arrayData == null ? 0L : 32L + arrayData.size() * 8L;
    }

    private long estimateMapBytes(MapData mapData) {
        return mapData == null ? 0L : 32L + mapData.size() * 16L;
    }

    private long estimateRawBytes(RawValueData<?> rawValueData) {
        return rawValueData == null ? 0L : 64L;
    }

    private class FlushWorker extends Thread {
        private ClickHouseBulkWriter writer;

        private FlushWorker(int workerIndex) {
            super("ck-batch-flush-worker-" + workerIndex);
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                openWriter();
                while (running || (flushQueue != null && !flushQueue.isEmpty())) {
                    BatchBuffer batchBuffer = pollBatch();
                    if (batchBuffer == null) {
                        continue;
                    }
                    try {
                        writeBatch(batchBuffer);
                    } finally {
                        onBatchFinished(batchBuffer);
                    }
                }
            } catch (Throwable t) {
                registerFailure(t);
            } finally {
                closeResources();
            }
        }

        private BatchBuffer pollBatch() throws InterruptedException {
            return flushQueue.poll(200L, TimeUnit.MILLISECONDS);
        }

        private void openWriter() throws Exception {
            this.writer = createWriter();
            this.writer.open();
        }

        private void rebuildWriter() throws Exception {
            if (this.writer != null) {
                this.writer.reopen();
            } else {
                openWriter();
            }
        }

        private void writeBatch(BatchBuffer batchBuffer) throws Exception {
            long startTime = System.nanoTime();
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    if (batchBuffer.hasEncodedBatch()) {
                        this.writer.write(batchBuffer.encodedBatch);
                    } else {
                        this.writer.write(batchBuffer.rows);
                    }
                    onBatchSucceeded(batchBuffer, startTime);
                    return;
                } catch (NonRetryableClickHouseException e) {
                    throw e;
                } catch (Exception e) {
                    LOG.error("ClickHouse batch write failed, attempt {}", attempt, e);
                    if (retryCounter != null) {
                        retryCounter.inc();
                    }
                    if (attempt >= maxRetries) {
                        throw e;
                    }
                    rebuildWriter();
                    sleepBeforeRetry(attempt);
                }
            }
        }

        private void onBatchSucceeded(BatchBuffer batchBuffer, long startTime) {
            if (flushCounter != null) {
                flushCounter.inc();
            }
            lastFlushRows = batchBuffer.rowCount;
            lastFlushBytes = batchBuffer.estimatedBytes;
            lastFlushLatencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        }

        private void sleepBeforeRetry(int attempt) throws IOException {
            try {
                Thread.sleep(1000L * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while retrying ClickHouse batch", e);
            }
        }

        private void onBatchFinished(BatchBuffer batchBuffer) {
            synchronized (stateLock) {
                bufferedRows -= batchBuffer.rowCount;
                bufferedBytes -= batchBuffer.estimatedBytes;
                inFlightBuffers--;
                stateLock.notifyAll();
            }
            // 整批发送结束后再归还共享预算，保证 in-flight 的真实占用也被计入控制范围。
            if (sharedMemoryLimiter != null && batchBuffer.hasEncodedBatch()) {
                sharedMemoryLimiter.release(batchBuffer.encodedBatch.size());
            }
        }

        private void closeResources() {
            if (this.writer != null) {
                try {
                    this.writer.close();
                } catch (Exception e) {
                    LOG.warn("failed to close ClickHouse bulk writer", e);
                } finally {
                    this.writer = null;
                }
            }
        }
    }

    private ClickHouseBulkWriter createWriter() {
        if ("http-rowbinary".equals(this.options.getWriterType())) {
            return new HttpRowBinaryWriter(
                    this.options,
                    this.connectionProvider,
                    this.directConnection,
                    this.targetUrl,
                    this.targetDatabaseName,
                    this.targetTableName,
                    this.fieldNames,
                    this.logicalTypes);
        }
        return new JdbcBatchWriter(this.sql, this.converter, this.connectionProvider, this.directConnection);
    }

    private class BatchBuffer {
        private final List<RowData> rows;
        private DynamicByteArrayOutputStream encodedBytesStream;
        private EncodedBatch encodedBatch;
        private final long createdNanos;
        private long estimatedBytes;
        private int rowCount;

        private BatchBuffer(long createdNanos) {
            this.createdNanos = createdNanos;
            this.rows = useEncodedBuffer ? null : new ArrayList<>();
            if (useEncodedBuffer) {
                // RowBinary 场景下只保留拼接中的字节流，不再缓存整批 RowData 对象。
                int initialCapacity = (int) Math.max(1024L, Math.min(batchBytes > 0 ? batchBytes : 64L * 1024L, 8L * 1024L * 1024L));
                this.encodedBytesStream = new DynamicByteArrayOutputStream(initialCapacity);
            }
        }

        private void add(RowData rowData, EncodedBatch encodedRow, long rowBytes) throws IOException {
            if (useEncodedBuffer) {
                if (encodedRow == null) {
                    throw new IOException("encoded row is required for rowbinary batches");
                }
                encodedRow.writeTo(this.encodedBytesStream);
            } else {
                this.rows.add(rowData);
            }
            this.rowCount++;
            this.estimatedBytes += rowBytes;
        }

        private boolean isEmpty() {
            return this.rowCount == 0;
        }

        private boolean hasEncodedBatch() {
            return this.encodedBatch != null;
        }

        private boolean shouldFlush() {
            return this.rowCount >= batchSize || (batchBytes > 0 && this.estimatedBytes >= batchBytes);
        }

        private void seal() {
            if (this.encodedBytesStream != null) {
                this.encodedBatch = this.encodedBytesStream.toEncodedBatch();
                this.encodedBytesStream = null;
            }
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final String name;

        private NamedThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, this.name);
            thread.setDaemon(true);
            return thread;
        }
    }
}
