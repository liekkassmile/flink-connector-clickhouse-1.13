package com.glab.flink.connector.clickhouse.table.internal.partitioner;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedInteger;
import org.apache.flink.table.data.RowData;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class HashPartitioner implements ClickHousePartitioner {
    private static final long serialVersionUID = 1L;

    private RowData.FieldGetter getter;

    public HashPartitioner(RowData.FieldGetter getter) {
        this.getter = getter;
    }

    //使用murmur3_32函数和clickhouse的murmurHash3_32保持统一
    public int select(RowData record, int numShards) {

        HashFunction hashFunction = Hashing.murmur3_32();
        long hashCode = Long.valueOf(UnsignedInteger.valueOf(
                Integer.toBinaryString(hashFunction.hashString(
                        this.getter.getFieldOrNull(record).toString(), StandardCharsets.UTF_8).asInt())
                , 2).toString());

        return Math.abs((int)(hashCode % numShards));
        // return Math.abs(Objects.hashCode(this.getter.getFieldOrNull(record)) % numShards);
    }
}
