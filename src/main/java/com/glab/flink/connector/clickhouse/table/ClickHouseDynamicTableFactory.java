package com.glab.flink.connector.clickhouse.table;

import com.glab.flink.connector.clickhouse.table.internal.dialect.ClickHouseDialect;
import com.glab.flink.connector.clickhouse.table.internal.options.ClickHouseOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.utils.TableSchemaUtils;

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

    public static final ConfigOption<Duration> SINK_FLUSH_INTERVAL = ConfigOptions.key("sink.flush-interval")
            .durationType()
            .defaultValue(Duration.ofSeconds(1L))
            .withDescription("the flush interval mills, over this time, asynchronous threads will flush data. The default value is 1s.");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES = ConfigOptions.key("sink.max-retries")
            .intType()
            .defaultValue(Integer.valueOf(3))
            .withDescription("the max retry times if writing records to database failed.");

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
        try {
            validateConfigOptions(config);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //带New的使用1.13API,不带的用12的
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return new ClickHouseDynamicTableSource(resolvedSchema, getOptions(config), getJdbcLookupOptions(config));

    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        try {
            validateConfigOptions(config);
        } catch (Exception e) {
            e.printStackTrace();
        }

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
        optionalOptions.add(SINK_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_WRITE_LOCAL);
        optionalOptions.add(SINK_PARTITION_STRATEGY);
        optionalOptions.add(SINK_PARTITION_KEY);
        optionalOptions.add(SINK_IGNORE_DELETE);
        optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(LOOKUP_CACHE_TTL);
        optionalOptions.add(LOOKUP_MAX_RETRIES);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) throws Exception{
        String partitionStrategy = config.get(SINK_PARTITION_STRATEGY);
        if (!Arrays.asList(new String[] { "hash", "balanced", "shuffle" }).contains(partitionStrategy))
            throw new IllegalArgumentException("Unknown sink.partition-strategy `" + partitionStrategy + "`");
        if (partitionStrategy.equals("hash") && !config.getOptional(SINK_PARTITION_KEY).isPresent())
            throw new IllegalArgumentException("A partition key must be provided for hash partition strategy");
        if ((config.getOptional(USERNAME).isPresent() ^ config.getOptional(PASSWORD).isPresent()))
            throw new IllegalArgumentException("Either all or none of username and password should be provided");
    }

    private ClickHouseOptions getOptions(ReadableConfig config) {
        return (new ClickHouseOptions.Builder()).withUrl((String)config.get(URL))
                .withUsername((String)config.get(USERNAME))
                .withPassword((String)config.get(PASSWORD))
                .withDatabaseName((String)config.get(DATABASE_NAME))
                .withTableName((String)config.get(TABLE_NAME))
                .withBatchSize(((Integer)config.get(SINK_BATCH_SIZE)).intValue())
                .withFlushInterval((Duration)config.get(SINK_FLUSH_INTERVAL))
                .withMaxRetries(((Integer)config.get(SINK_MAX_RETRIES)).intValue())
                .withWriteLocal((Boolean)config.get(SINK_WRITE_LOCAL))
                .withPartitionStrategy((String)config.get(SINK_PARTITION_STRATEGY))
                .withPartitionKey((String)config.get(SINK_PARTITION_KEY))
                .withIgnoreDelete(((Boolean)config.get(SINK_IGNORE_DELETE)).booleanValue())
                .setDialect(new ClickHouseDialect())
                .build();
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
