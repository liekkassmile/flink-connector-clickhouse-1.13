package com.glab.flink.connector.clickhouse.table.internal.partitioner;

import org.apache.flink.table.data.RowData;

import java.util.concurrent.ThreadLocalRandom;

public class ShufflePartitioner implements ClickHousePartitioner {
    private static final long serialVersionUID = 1L;

    public int select(RowData record, int numShards) {
        return ThreadLocalRandom.current().nextInt(numShards);
    }
}
