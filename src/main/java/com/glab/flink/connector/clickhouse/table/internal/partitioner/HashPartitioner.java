package com.glab.flink.connector.clickhouse.table.internal.partitioner;

import org.apache.flink.table.data.RowData;

import java.util.Objects;

public class HashPartitioner implements ClickHousePartitioner {
    private static final long serialVersionUID = 1L;

    private RowData.FieldGetter getter;

    public HashPartitioner(RowData.FieldGetter getter) {
        this.getter = getter;
    }

    public int select(RowData record, int numShards) {
        //return Objects.hashCode(this.getter.getFieldOrNull(record)) % numShards;
        return Math.abs(Objects.hashCode(this.getter.getFieldOrNull(record)) % numShards);
    }
}
