package com.glab;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author lrh
 * @date 2021/6/22
 */
public class TestKafka {
    public static void main(String[] args) {

        Configuration config = new Configuration();
        config.setString("taskmanager.numberOfTaskSolots", "1");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // tenv.registerFunction("redisLookup", new RedisLookupFunction());

        tenv.executeSql("CREATE TABLE ck_sink (\n" +
                "    name VARCHAR,\n" +
                "    grade VARCHAR,\n" +
                "    rate VARCHAR,\n" +
                "    more VARCHAR\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'clickhouse://xxxx:8123',\n" +
                "    'username' = '',\n" +
                "    'password' = '',\n" +
                "    'database-name' = 'glab',        /* ClickHouse 数据库名，默认为 default */\n" +
                "    'table-name' = 'ck_test',      /* ClickHouse 数据表名 */\n" +
                "    'sink.batch-size' = '10',         /* batch 大小 */\n" +
                "    'sink.flush-interval' = '1000',     /* flush 时间间隔 */\n" +
                "    'sink.max-retries' = '1',           /* 最大重试次数 */\n" +
                "    'sink.partition-strategy' = 'balanced', /* hash | shuffle | balanced */\n" +
                "    'sink.write-local' = 'true',\n" +
                "    'sink.ignore-delete' = 'true'       /* 忽略 DELETE 并视 UPDATE 为 INSERT */\n" +
                ")");

        tenv.executeSql("create table if not exists ck_kafka(\n" +
                "\tname VARCHAR,\n" +
                "\tgrade VARCHAR,\n" +
                "\trate VARCHAR,\n" +
                "\tmore VARCHAR\n" +
                ")WITH(\n" +
                "\t'connector' = 'kafka',\n" +
                "\t'topic' = 'ck_test',\n" +
                "\t'scan.startup.mode' = 'latest-offset',\n" +
                "\t'properties.group.id' = 'ck_test1',\n" +
                "\t'properties.bootstrap.servers' = '***:9092,***:9092',\n" +
                "\t'format' = 'csv',\n" +
                "\t'csv.ignore-parse-errors' = 'true',\n" +
                "\t'csv.field-delimiter' = '|',\n" +
                "\t'csv.null-literal' = ''\n" +
                ")");


        //tenv.sqlQuery("select * from profile_ids_merge_ck limit 10").execute().print();
        //tenv.executeSql("select a.*,b.* from ck_kafka a left join ck_sink b on a.more = b.more where b.more <> '' limit 2").print();
        tenv.executeSql("insert into ck_sink select * from ck_kafka ").print();

    }
}
