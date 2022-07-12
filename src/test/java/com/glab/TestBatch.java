package com.glab;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Unit test for simple App.
 */
public class TestBatch {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setDefaultLocalParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //StreamTableEnvironment benv = StreamTableEnvironment.create(env, settings);


        EnvironmentSettings bes = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment benv = TableEnvironment.create(bes);


         //tenv.registerFunction("redisLookup", new RedisLookupFunction());

        benv.executeSql("CREATE TABLE profile_ids_property_ck (\n" +
                "    name String,\n" +
                "    grade String, \n" +
                "    rate String, \n" +
                "    more String\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'clickhouse://192.168.8.94:8123',\n" +
                "    'username' = 'default',\n" +
                "    'password' = 'gexin2011',\n" +
                "    'database-name' = 'glab',        /* ClickHouse 数据库名，默认为 default */\n" +
                "    'table-name' = 'ck_test',      /* ClickHouse 数据表名 */\n" +
                "    'sink.batch-size' = '1000',         /* batch 大小 */\n" +
                "    'sink.flush-interval' = '3000',     /* flush 时间间隔 */\n" +
                "    'sink.max-retries' = '3',           /* 最大重试次数 */\n" +
                "    'sink.partition-strategy' = 'hash', /* hash | shuffle | balanced */\n" +
                 "   'sink.partition-key' = 'name',\n" +
                "    'sink.write-local' = 'true',\n" +
                "    'sink.ignore-delete' = 'true',       /* 忽略 DELETE 并视 UPDATE 为 INSERT */\n" +
                "    'lookup.cache.max-rows' = '100',\n" +
                "    'lookup.cache.ttl' = '10',\n" +
                "    'lookup.max-retries' = '3'\n" +
                ")");

        benv.executeSql("CREATE TABLE id_property_std (\n" +
                "   name STRING,\n" +
                "   grade STRING, \n" +
                "   rate STRING,\n" +
                "   more STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'filesystem',\n" +
                "    'path'= 'file:/mysource/gt/data/part-042f34ec-8194-4bfc-88c8-170a41aa402e-0-0',\n" +
                "    'format' = 'csv',\n" +
                "    'csv.field-delimiter' = '|'\n" +
                "    )");
        /**benv.executeSql("insert overwrite  id_property_std values " +
                "('1', 'man', '1', 'hello'),('1', 'woman', '1', 'world'),\n" +
                "('2', 'man', '2', 'hello'),('2', 'woman', '2', 'world'),\n" +
                "('3', 'man', '3', 'hello'),('3', 'woman', '3', 'world')");**/
        //tenv.sqlQuery("select * from profile_ids_merge_ck limit 10").execute().print();
        //tenv.executeSql("select a.*,b.* from profile_ids_merge_ck a left join ck_sink b on a.source = b.more where b.more <> '' limit 2").print();
        //tenv.executeSql("select * from id_property_std limit 10").print();
        benv.executeSql("insert into  profile_ids_property_ck select name, grade, '3' as rate, more from id_property_std").print();
    }
}
