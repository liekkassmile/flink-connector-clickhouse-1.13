package com.glab.flink.connector.clickhouse.table.internal.dialect;

import com.glab.flink.connector.clickhouse.table.internal.ClickHouseStatementFactory;
import com.glab.flink.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * ck删除，更新，插入区分集群表和本地表
 * @author lrh
 * @date 2021/6/21
 */
public class ClickHouseDialect implements JdbcDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:clickhouse:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new ClickHouseRowConverter(rowType);
    }


    //1.13的
    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
    }

    public String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns = Arrays.stream(fieldNames).map(ClickHouseStatementFactory::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fieldNames).map(f -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + quoteIdentifier(tableName) + "(" + columns + ") VALUES (" + placeholders + ")";
    }

    public String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields, Optional<String> clusterName) {
        String setClause = Arrays.stream(fieldNames).map(f -> quoteIdentifier(f) + "= ?").collect(Collectors.joining(", "));
        String conditionClause = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "= ?").collect(Collectors.joining(" AND "));
        String onClusterClause = "";
        if(clusterName.isPresent()) {
            onClusterClause = " ON CLAUSTER " + quoteIdentifier(clusterName.get());
        }

        return "ALTER TABLE " + quoteIdentifier(tableName) + onClusterClause + " UPDATE " + setClause + " WHERE " + conditionClause;
    }

    public String getDeleteStatement(String tableName, String[] conditionFields, Optional<String> clusterName) {
        String conditionClause = Arrays.<String>stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
        String onClusterClause = "";
        if (clusterName.isPresent())
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName.get());
        return "ALTER TABLE " + quoteIdentifier(tableName) + onClusterClause + " DELETE WHERE " + conditionClause;
    }

    public String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
    }

    public String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
        String selectExpressions = (String)Arrays.stream(selectFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String fieldExpressions = (String)Arrays.stream(conditionFields).map((f) -> {
            return String.format("%s = :%s", this.quoteIdentifier(f), f);
        }).collect(Collectors.joining(" AND "));
        return "SELECT " + selectExpressions + " FROM " + this.quoteIdentifier(tableName) + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
