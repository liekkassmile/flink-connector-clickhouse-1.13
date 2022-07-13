package com.glab;

import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author lrh
 * @date 2021/7/8
 */
public class TestJdbc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // tenv.registerFunction("redisLookup", new RedisLookupFunction());
        tenv.executeSql("CREATE TABLE mysql_test (\n" +
                "    user_id STRING,\n" +
                "    item_id STRING,\n" +
                "    category_id STRING,\n" +
                "    behavior STRING,\n" +
                "    ts STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://xxxx:3306/allway',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '',\n" +
                "    'table-name' = 'testdata'      /* ClickHouse 数据表名 */\n" +
                ")");


        //tenv.sqlQuery("select * from profile_ids_merge_ck limit 10").execute().print();
        //tenv.executeSql("select a.*,b.* from profile_ids_merge_ck a left join ck_sink b on a.source = b.more where b.more <> '' limit 2").print();
        tenv.executeSql("select * from mysql_test limit 10").print();
    }
}
