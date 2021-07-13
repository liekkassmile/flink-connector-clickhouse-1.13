package com.glab.flink.connector.clickhouse.table.internal;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class ClickHouseStatementFactory {
    public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns = Arrays.stream(fieldNames).map(ClickHouseStatementFactory::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fieldNames).map(f -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + quoteIdentifier(tableName) + "(" + columns + ") VALUES (" + placeholders + ")";
    }

    public static String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields, Optional<String> clusterName) {
        String setClause = Arrays.stream(fieldNames).map(f -> quoteIdentifier(f) + "= ?").collect(Collectors.joining(", "));
        String conditionClause = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "= ?").collect(Collectors.joining(" AND "));
        String onClusterClause = "";
        if(clusterName.isPresent()) {
            onClusterClause = " ON CLAUSTER " + quoteIdentifier(clusterName.get());
        }

        return "ALTER TABLE " + quoteIdentifier(tableName) + onClusterClause + " UPDATE " + setClause + " WHERE " + conditionClause;
    }

    public static String getDeleteStatement(String tableName, String[] conditionFields, Optional<String> clusterName) {
        String conditionClause = Arrays.<String>stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
        String onClusterClause = "";
        if (clusterName.isPresent())
            onClusterClause = " ON CLUSTER " + quoteIdentifier(clusterName.get());
        return "ALTER TABLE " + quoteIdentifier(tableName) + onClusterClause + " DELETE WHERE " + conditionClause;
    }

    public static String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions = Arrays.stream(conditionFields).map(f -> quoteIdentifier(f) + "=?").collect(Collectors.joining(" AND "));
        return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
    }

    public static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
