package com.glab.flink.connector.clickhouse.table.internal.executor;

import java.io.OutputStream;
import java.util.Arrays;

public class DynamicByteArrayOutputStream extends OutputStream {
    // 可复用的动态字节缓冲，编码单行和拼接整批时都复用这套逻辑。
    private byte[] buffer;
    private int count;

    public DynamicByteArrayOutputStream(int initialCapacity) {
        this.buffer = new byte[Math.max(32, initialCapacity)];
        this.count = 0;
    }

    @Override
    public void write(int b) {
        ensureCapacity(this.count + 1);
        this.buffer[this.count++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if (b == null || len <= 0) {
            return;
        }
        ensureCapacity(this.count + len);
        System.arraycopy(b, off, this.buffer, this.count, len);
        this.count += len;
    }

    public int size() {
        return this.count;
    }

    public void reset() {
        this.count = 0;
    }

    public EncodedBatch toEncodedBatch() {
        return new EncodedBatch(Arrays.copyOf(this.buffer, this.count), this.count);
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity <= this.buffer.length) {
            return;
        }
        int newCapacity = this.buffer.length;
        while (newCapacity < minCapacity) {
            newCapacity = newCapacity << 1;
        }
        this.buffer = Arrays.copyOf(this.buffer, newCapacity);
    }
}
