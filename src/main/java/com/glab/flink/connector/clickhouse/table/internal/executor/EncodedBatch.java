package com.glab.flink.connector.clickhouse.table.internal.executor;

import java.io.IOException;
import java.io.OutputStream;

public class EncodedBatch {
    // 可重放的整批编码结果，用于 HTTP 重试时重复发送同一批数据。
    private final byte[] bytes;
    private final int size;

    public EncodedBatch(byte[] bytes, int size) {
        this.bytes = bytes;
        this.size = size;
    }

    public int size() {
        return this.size;
    }

    public void writeTo(OutputStream outputStream) throws IOException {
        outputStream.write(this.bytes, 0, this.size);
    }
}
