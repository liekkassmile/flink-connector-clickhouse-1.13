package com.glab.flink.connector.clickhouse.table.internal.partitioner;

import org.apache.flink.table.data.RowData;

public class BalancedPartitioner implements ClickHousePartitioner {
    private static final long serialVersionUID = 1L;

    private int nextShard = 0;

    public int select(RowData record, int numShards) {
        this.nextShard = (this.nextShard + 1) % numShards;
        return this.nextShard;
    }
}
