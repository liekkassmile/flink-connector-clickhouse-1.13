package com.glab;

import static org.junit.Assert.assertTrue;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class Test01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // tenv.registerFunction("redisLookup", new RedisLookupFunction());

        tenv.executeSql("CREATE TABLE profile_ids_merge_ck (\n" +
                "    ts BIGINT,\n" +
                "    id_idtype_key VARCHAR,\n" +
                "    id_idtype_value VARCHAR,\n" +
                "    source VARCHAR,\n" +
                "    date_partition VARCHAR\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'clickhouse://xxxx:8123',\n" +
                "    'username' = '',\n" +
                "    'password' = '',\n" +
                "    'database-name' = 'glab',        /* ClickHouse 数据库名，默认为 default */\n" +
                "    'table-name' = 'profile_ids_merge',      /* ClickHouse 数据表名 */\n" +
                "    'sink.batch-size' = '1000',         /* batch 大小 */\n" +
                "    'sink.flush-interval' = '1000',     /* flush 时间间隔 */\n" +
                "    'sink.max-retries' = '3',           /* 最大重试次数 */\n" +
                "    'lookup.cache.max-rows' = '100',\n" +
                "    'lookup.cache.ttl' = '10',\n" +
                "    'lookup.max-retries' = '3'\n" +
                ")");

        tenv.executeSql("CREATE TABLE ck_sink (\n" +
                "    name VARCHAR,\n" +
                "    grade BIGINT,\n" +
                "    rate DOUBLE,\n" +
                "    more VARCHAR\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'clickhouse://xxxx:8123',\n" +
                "    'username' = '',\n" +
                "    'password' = '',\n" +
                "    'database-name' = 'glab',        /* ClickHouse 数据库名，默认为 default */\n" +
                "    'table-name' = 'ck_test',      /* ClickHouse 数据表名 */\n" +
                "    'sink.batch-size' = '50',         /* batch 大小 */\n" +
                "    'sink.flush-interval' = '1000',     /* flush 时间间隔 */\n" +
                "    'sink.max-retries' = '1',           /* 最大重试次数 */\n" +
                "    'sink.partition-strategy' = 'balanced', /* hash | shuffle | balanced */\n" +
                "    'sink.write-local' = 'true',\n" +
                "    'sink.ignore-delete' = 'true',       /* 忽略 DELETE 并视 UPDATE 为 INSERT */\n" +
                "    'lookup.cache.max-rows' = '100',\n" +
                "    'lookup.cache.ttl' = '10',\n" +
                "    'lookup.max-retries' = '3'\n" +
                ")");


        //tenv.sqlQuery("select * from profile_ids_merge_ck limit 10").execute().print();
        //tenv.executeSql("select a.*,b.* from profile_ids_merge_ck a left join ck_sink b on a.source = b.more where b.more <> '' limit 2").print();
        tenv.executeSql("select * from profile_ids_merge_ck limit 10").print();
    }
}
