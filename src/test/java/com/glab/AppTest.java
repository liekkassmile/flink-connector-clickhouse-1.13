package com.glab;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // tenv.registerFunction("redisLookup", new RedisLookupFunction());

        tenv.executeSql("CREATE TABLE ck_sink (\n" +
                "    name VARCHAR,\n" +
                "    grade BIGINT,\n" +
                "    rate FLOAT,\n" +
                "    more VARCHAR\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'clickhouse://192.168.8.94:8123',\n" +
                "    'username' = '',\n" +
                "    'password' = '',\n" +
                "    'database-name' = 'glab',        /* ClickHouse 数据库名，默认为 default */\n" +
                "    'table-name' = 'ck_test',      /* ClickHouse 数据表名 */\n" +
                "    'sink.batch-size' = '20',         /* batch 大小 */\n" +
                "    'sink.flush-interval' = '1000',     /* flush 时间间隔 */\n" +
                "    'sink.max-retries' = '2',           /* 最大重试次数 */\n" +
                "    'sink.partition-strategy' = 'balanced', /* hash | shuffle | balanced */\n" +
                "    'sink.write-local' = 'true',\n" +
                "    'sink.ignore-delete' = 'true'       /* 忽略 DELETE 并视 UPDATE 为 INSERT */\n" +
                ")");

        tenv.executeSql("create table if not exists ck_datagen(\n" +
                "\tname VARCHAR,\n" +
                "\tgrade BIGINT,\n" +
                "\trate FLOAT,\n" +
                "\tmore VARCHAR\n" +
                ")WITH(\n" +
                "\t'connector' = 'datagen',\n" +
                "\t'rows-per-second' = '10',\n" +
                "\t'fields.name.length' = '3',\n" +
                "\t'fields.grade.min' = '10',\n" +
                "\t'fields.grade.max' = '30',\n" +
                "\t'fields.rate.min' = '0',\n" +
                "\t'fields.rate.max' = '100',\n" +
                "\t'fields.more.length' = '9'\n" +
                ")");

        tenv.executeSql("create table if not exists ck_kafka(\n" +
                "\tname VARCHAR,\n" +
                "\tgrade BIGINT,\n" +
                "\trate FLOAT,\n" +
                "\tmore VARCHAR\n" +
                ")WITH(\n" +
                "\t'connector' = 'kafka',\n" +
                "\t'topic' = 'ck_test',\n" +
                "\t'scan.startup.mode' = 'latest-offset',\n" +
                "\t'properties.group.id' = 'ck_test1',\n" +
                "\t'properties.bootstrap.servers' = 'xxxx:9092,xxxx:9092',\n" +
                "\t'format' = 'csv',\n" +
                "\t'csv.ignore-parse-errors' = 'true',\n" +
                "\t'csv.field-delimiter' = '|',\n" +
                "\t'csv.null-literal' = ''\n" +
                ")");

         tenv.executeSql("insert into ck_sink select * from ck_datagen");
        // tenv.executeSql("insert into redis_set values('s1', Array['a','c2','3'])");
        //tenv.executeSql("insert into ck_sink values('jim3', 10, 0.1, 'hello world'),('alice3', 12, 0.1, 'hello ss')," +
               // "('jude3', 12, 0.1, 'hello ss'),('android3', 12, 0.1, 'hello ss'),('jack3', 12, 0.1, 'hello ss')").print();
        //tenv.executeSql("insert into ck_sink values('jim2', 10, 0.1, 'hello world')").print();
    }
}
