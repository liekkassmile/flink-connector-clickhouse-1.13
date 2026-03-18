package com.glab;

import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;


/**
 * @author lrh
 * @date 2021/7/8
 */
public class TestJdbc {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//
//        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
//
//        // tenv.registerFunction("redisLookup", new RedisLookupFunction());
//        tenv.executeSql("CREATE TABLE mysql_test (\n" +
//                "    user_id STRING,\n" +
//                "    item_id STRING,\n" +
//                "    category_id STRING,\n" +
//                "    behavior STRING,\n" +
//                "    ts STRING\n" +
//                ") WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://xxxx:3306/allway',\n" +
//                "    'username' = 'root',\n" +
//                "    'password' = '',\n" +
//                "    'table-name' = 'testdata'      /* ClickHouse 数据表名 */\n" +
//                ")");
//
//
//        //tenv.sqlQuery("select * from profile_ids_merge_ck limit 10").execute().print();
//        //tenv.executeSql("select a.*,b.* from profile_ids_merge_ck a left join ck_sink b on a.source = b.more where b.more <> '' limit 2").print();
//        tenv.executeSql("select * from mysql_test limit 10").print();

        String s = DigestUtils.md5Hex("411024199211081613");
        System.out.println(s);

        String document = "这是一个示例文档，包含了一些关键词，如Java、分词器、IK Analyzer等。";

        document = "This is a sample document containing keywords like Java, tokenizer, and Lucene.";

        String[] keywords = {"Java", "tokenizer", "Lucene"};

        Analyzer analyzer = new StandardAnalyzer("");

        TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(document));
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);

        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            String token = charTermAttribute.toString();
            for (String keyword : keywords) {
                if (token.equalsIgnoreCase(keyword)) {
                    System.out.println("Matched keyword: " + keyword);
                }
            }
        }
        tokenStream.end();
        tokenStream.close();

        analyzer.close();
    }
 }
