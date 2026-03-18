package com.glab.flink.connector.clickhouse.table;

import com.glab.flink.connector.clickhouse.table.internal.dialect.ClickHouseDialect;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ClickHouseDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {
    public static final String IDENTIFIER = "clickhouse";

    private static final String DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

    public static final ConfigOption<String> URL = ConfigOptions.key("url")
            .stringType()
            .noDefaultValue()
            .withDeprecatedKeys("the ClickHouse url in format `clickhouse://<host>:<port>`.");
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the ClickHouse username.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the ClickHouse password.");

    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
            .stringType()
            .defaultValue("default")
            .withDescription("the ClickHouse database name. Default to `default`.");

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the ClickHouse table name.");

    public static final ConfigOption<Integer> SINK_BATCH_SIZE = ConfigOptions.key("sink.batch-size")
            .intType()
            .defaultValue(Integer.valueOf(1000))
            .withDescription("the flush max size, over this number of records, will flush data. The default value is 1000.");

    public static final ConfigOption<Long> SINK_BATCH_BYTES = ConfigOptions.key("sink.batch-bytes")
            .longType()
            .defaultValue(0L)
            .withDescription("flush batch when buffered bytes reach this threshold. 0 means disabled.");

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL = ConfigOptions.key("sink.flush-interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1L))
            .withDescription("the flush interval mills, over this time, asynchronous threads will flush data. The default value is 1s.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions.key("sink.max-retries")
            .intType()
            .defaultValue(Integer.valueOf(3))
            .withDescription("the max retry times if writing records to database failed.");

    public static final ConfigOption<Integer> SINK_MAX_BUFFERED_ROWS = ConfigOptions.key("sink.max-buffered-rows")
            .intType()
            .defaultValue(0)
            .withDescription("max rows allowed in memory before backpressure. 0 means use sink.batch-size * 3.");

    public static final ConfigOption<Integer> SINK_MAX_IN_FLIGHT_BATCHES = ConfigOptions.key("sink.max-inflight-batches")
            .intType()
            .defaultValue(1)
            .withDescription("max number of batches allowed to be queued or writing concurrently.");

    public static final ConfigOption<Integer> SINK_FLUSH_THREAD_NUM = ConfigOptions.key("sink.flush-thread-num")
            .intType()
            .defaultValue(1)
            .withDescription("number of flush worker threads used by the batch sink.");

    public static final ConfigOption<Boolean> SINK_PREFER_LARGE_BATCH = ConfigOptions.key("sink.prefer-large-batch")
            .booleanType()
            .defaultValue(Boolean.FALSE)
            .withDescription("delay time-based flush slightly when the current batch is still too small.");

    public static final ConfigOption<Integer> SINK_PARTIAL_FLUSH_MIN_ROWS = ConfigOptions.key("sink.partial-flush-min-rows")
            .intType()
            .defaultValue(0)
            .withDescription("minimum rows expected for a timer-driven flush before delaying once more. 0 means sink.batch-size / 5.");

    public static final ConfigOption<String> SINK_WRITER_TYPE = ConfigOptions.key("sink.writer-type")
            .stringType()
            .defaultValue("jdbc")
            .withDescription("sink writer implementation. available: jdbc, http-rowbinary.");

    public static final ConfigOption<Duration> SINK_HTTP_CONNECT_TIMEOUT = ConfigOptions.key("sink.http.connect-timeout")
            .durationType()
            .defaultValue(Duration.ofSeconds(10))
            .withDescription("http connect timeout used by http-rowbinary writer.");

    public static final ConfigOption<Duration> SINK_HTTP_SOCKET_TIMEOUT = ConfigOptions.key("sink.http.socket-timeout")
            .durationType()
            .defaultValue(Duration.ofSeconds(30))
            .withDescription("http socket timeout used by http-rowbinary writer.");

    public static final ConfigOption<Boolean> SINK_WRITE_LOCAL = ConfigOptions.key("sink.write-local")
            .booleanType()
            .defaultValue(Boolean.valueOf(false))
            .withDescription("directly write to local tables in case of Distributed table.");

    public static final ConfigOption<String> SINK_PARTITION_STRATEGY = ConfigOptions.key("sink.partition-strategy")
            .stringType()
            .defaultValue("balanced")
            .withDescription("partition strategy. available: balanced, hash, shuffle.");

    public static final ConfigOption<String> SINK_PARTITION_KEY = ConfigOptions.key("sink.partition-key")
            .stringType()
            .noDefaultValue()
            .withDescription("partition key used for hash strategy.");

    public static final ConfigOption<Boolean> SINK_IGNORE_DELETE = ConfigOptions.key("sink.ignore-delete")
            .booleanType()
            .defaultValue(Boolean.valueOf(true))
            .withDescription("whether to treat update statements as insert statements and ignore deletes. defaults to true.");

    public static final ConfigOption<String> SINK_MODE = ConfigOptions.key("sink.mode")
            .stringType()
            .defaultValue("normal")
            .withDescription("sink mode. available: normal, clickhouse-friendly.");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions.key("lookup.cache.max-rows")
            .longType()
            .defaultValue(-1L)
            .withDescription("the max number of rows of lookup cache, over this value, the oldest rows will be eliminated." +
                    "cache.max-rows and cache ttl options must all be specified id any of them is specified. cache is not enabled as default.");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions.key("lookup.cache.ttl")
            .durationType()
            .defaultValue(Duration.ofSeconds(10))
            .withDescription("the cache time to live");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions.key("lookup.max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("the max retry times if lookup database failed.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);

        //带New的使用1.13API,不带的用12的
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new ClickHouseDynamicTableSource(resolvedSchema, getOptions(config), getJdbcLookupOptions(config));

    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);

        //带New的使用1.13API,不带的用12的
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new ClickHouseDynamicTableSink(resolvedSchema, getOptions(config));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(DATABASE_NAME);
        optionalOptions.add(SINK_BATCH_SIZE);
        optionalOptions.add(SINK_BATCH_BYTES);
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_MAX_BUFFERED_ROWS);
        optionalOptions.add(SINK_MAX_IN_FLIGHT_BATCHES);
        optionalOptions.add(SINK_FLUSH_THREAD_NUM);
        optionalOptions.add(SINK_PREFER_LARGE_BATCH);
        optionalOptions.add(SINK_PARTIAL_FLUSH_MIN_ROWS);
        optionalOptions.add(SINK_WRITER_TYPE);
        optionalOptions.add(SINK_HTTP_CONNECT_TIMEOUT);
        optionalOptions.add(SINK_HTTP_SOCKET_TIMEOUT);
        optionalOptions.add(SINK_WRITE_LOCAL);
        optionalOptions.add(SINK_PARTITION_STRATEGY);
        optionalOptions.add(SINK_PARTITION_KEY);
        optionalOptions.add(SINK_IGNORE_DELETE);
        optionalOptions.add(SINK_MODE);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {
        String partitionStrategy = config.get(SINK_PARTITION_STRATEGY);
        if (!Arrays.asList(new String[] { "hash", "balanced", "shuffle" }).contains(partitionStrategy))
            throw new IllegalArgumentException("Unknown sink.partition-strategy `" + partitionStrategy + "`");
        String sinkMode = config.get(SINK_MODE);
        if (!Arrays.asList(new String[] { "normal", "clickhouse-friendly" }).contains(sinkMode))
            throw new IllegalArgumentException("Unknown sink.mode `" + sinkMode + "`");
        String writerType = config.get(SINK_WRITER_TYPE);
        if (!Arrays.asList(new String[] { "jdbc", "http-rowbinary" }).contains(writerType))
            throw new IllegalArgumentException("Unknown sink.writer-type `" + writerType + "`");
        if (partitionStrategy.equals("hash") && !config.getOptional(SINK_PARTITION_KEY).isPresent())
            throw new IllegalArgumentException("A partition key must be provided for hash partition strategy");
        if ((config.getOptional(USERNAME).isPresent() ^ config.getOptional(PASSWORD).isPresent()))
            throw new IllegalArgumentException("Either all or none of username and password should be provided");
        if (config.get(SINK_MAX_IN_FLIGHT_BATCHES) < 1)
            throw new IllegalArgumentException("sink.max-inflight-batches must be >= 1");
        if (config.get(SINK_FLUSH_THREAD_NUM) < 1)
            throw new IllegalArgumentException("sink.flush-thread-num must be >= 1");
        if ("http-rowbinary".equals(writerType) && !config.get(SINK_IGNORE_DELETE))
            throw new IllegalArgumentException("sink.writer-type=http-rowbinary only supports append-only semantics. Please set sink.ignore-delete=true.");
    }

    private ClickHouseOptions getOptions(ReadableConfig config) {
        boolean clickHouseFriendly = "clickhouse-friendly".equals(config.get(SINK_MODE));
        int batchSize = resolveInt(config, SINK_BATCH_SIZE, clickHouseFriendly ? 100000 : SINK_BATCH_SIZE.defaultValue());
        long batchBytes = resolveLong(config, SINK_BATCH_BYTES, clickHouseFriendly ? 64L * 1024 * 1024 : SINK_BATCH_BYTES.defaultValue());
        Duration flushInterval = resolveDuration(config, SINK_FLUSH_INTERVAL, clickHouseFriendly ? Duration.ofSeconds(10L) : SINK_FLUSH_INTERVAL.defaultValue());
        int maxBufferedRows = resolveInt(config, SINK_MAX_BUFFERED_ROWS, clickHouseFriendly ? 300000 : Math.max(batchSize * 3, batchSize));
        int maxInFlightBatches = resolveInt(config, SINK_MAX_IN_FLIGHT_BATCHES, clickHouseFriendly ? 1 : SINK_MAX_IN_FLIGHT_BATCHES.defaultValue());
        int flushThreadNum = resolveInt(config, SINK_FLUSH_THREAD_NUM, clickHouseFriendly ? 1 : SINK_FLUSH_THREAD_NUM.defaultValue());
        boolean preferLargeBatch = resolveBoolean(config, SINK_PREFER_LARGE_BATCH, clickHouseFriendly);
        int partialFlushMinRows = resolveInt(config, SINK_PARTIAL_FLUSH_MIN_ROWS, clickHouseFriendly ? Math.max(20000, batchSize / 5) : Math.max(0, batchSize / 5));
        String writerType = resolveString(config, SINK_WRITER_TYPE, clickHouseFriendly ? "http-rowbinary" : SINK_WRITER_TYPE.defaultValue());
        Duration httpConnectTimeout = resolveDuration(config, SINK_HTTP_CONNECT_TIMEOUT, SINK_HTTP_CONNECT_TIMEOUT.defaultValue());
        Duration httpSocketTimeout = resolveDuration(config, SINK_HTTP_SOCKET_TIMEOUT, SINK_HTTP_SOCKET_TIMEOUT.defaultValue());
        boolean writeLocal = resolveBoolean(config, SINK_WRITE_LOCAL, clickHouseFriendly);
        boolean ignoreDelete = resolveBoolean(config, SINK_IGNORE_DELETE, clickHouseFriendly || SINK_IGNORE_DELETE.defaultValue());

        return (new ClickHouseOptions.Builder()).withUrl((String)config.get(URL))
                .withUsername((String)config.get(USERNAME))
                .withPassword((String)config.get(PASSWORD))
                .withDatabaseName((String)config.get(DATABASE_NAME))
                .withTableName((String)config.get(TABLE_NAME))
                .withBatchSize(batchSize)
                .withBatchBytes(batchBytes)
                .withFlushInterval(flushInterval)
                .withMaxRetries(((Integer)config.get(SINK_MAX_RETRIES)).intValue())
                .withMaxBufferedRows(maxBufferedRows)
                .withMaxInFlightBatches(maxInFlightBatches)
                .withFlushThreadNum(flushThreadNum)
                .withPreferLargeBatch(preferLargeBatch)
                .withPartialFlushMinRows(partialFlushMinRows)
                .withWriterType(writerType)
                .withHttpConnectTimeout(httpConnectTimeout)
                .withHttpSocketTimeout(httpSocketTimeout)
                .withWriteLocal(writeLocal)
                .withPartitionStrategy((String)config.get(SINK_PARTITION_STRATEGY))
                .withPartitionKey((String)config.get(SINK_PARTITION_KEY))
                .withIgnoreDelete(ignoreDelete)
                .withSinkMode(config.get(SINK_MODE))
                .setDialect(new ClickHouseDialect())
                .build();
    }

    private int resolveInt(ReadableConfig config, ConfigOption<Integer> option, int fallbackValue) {
        return config.getOptional(option).orElse(fallbackValue);
    }

    private long resolveLong(ReadableConfig config, ConfigOption<Long> option, long fallbackValue) {
        return config.getOptional(option).orElse(fallbackValue);
    }

    private boolean resolveBoolean(ReadableConfig config, ConfigOption<Boolean> option, boolean fallbackValue) {
        return config.getOptional(option).orElse(fallbackValue);
    }

    private String resolveString(ReadableConfig config, ConfigOption<String> option, String fallbackValue) {
        return config.getOptional(option).orElse(fallbackValue);
    }

    private Duration resolveDuration(ReadableConfig config, ConfigOption<Duration> option, Duration fallbackValue) {
        return config.getOptional(option).orElse(fallbackValue);
    }

/*    private JdbcOptions getJdbcOptions(ReadableConfig config) {
        return JdbcOptions.builder()
                .setDriverName(DRIVER_NAME)
                .setDBUrl(config.get(URL))
                .setTableName(config.get(TABLE_NAME))
                .setDialect(new ClickHouseDialect())
                .build();
    }*/


    private JdbcLookupOptions getJdbcLookupOptions(ReadableConfig config) {
        return JdbcLookupOptions.builder()
                .setCacheExpireMs(config.get(LOOKUP_CACHE_TTL).toMillis())
                .setMaxRetryTimes(config.get(LOOKUP_MAX_RETRIES))
                .setCacheMaxSize(config.get(LOOKUP_CACHE_MAX_ROWS))
                .build();
    }

}
