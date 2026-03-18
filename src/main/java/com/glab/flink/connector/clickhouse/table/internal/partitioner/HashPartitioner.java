package com.glab.flink.connector.clickhouse.table.internal.partitioner;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.RowData;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class HashPartitioner implements ClickHousePartitioner {
    private static final long serialVersionUID = 1L;

    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();

    private RowData.FieldGetter getter;

    public HashPartitioner(RowData.FieldGetter getter) {
        this.getter = getter;
    }

    //使用murmur3_32函数和clickhouse的murmurHash3_32保持统一
    public int select(RowData record, int numShards) {
        Object key = this.getter.getFieldOrNull(record);
        if (key == null) {
            return 0;
        }

        int hashCode;
        if (key instanceof Integer) {
            hashCode = HASH_FUNCTION.hashInt((Integer) key).asInt();
        } else if (key instanceof Long) {
            hashCode = HASH_FUNCTION.hashLong((Long) key).asInt();
        } else if (key instanceof byte[]) {
            hashCode = HASH_FUNCTION.hashBytes((byte[]) key).asInt();
        } else if (key instanceof StringData) {
            hashCode = HASH_FUNCTION.hashString(key.toString(), StandardCharsets.UTF_8).asInt();
        } else {
            hashCode = HASH_FUNCTION.hashInt(Objects.hashCode(key)).asInt();
        }

        return Integer.remainderUnsigned(hashCode, numShards);
    }
}
