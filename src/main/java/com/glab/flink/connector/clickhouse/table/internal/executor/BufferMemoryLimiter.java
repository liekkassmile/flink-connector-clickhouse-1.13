package com.glab.flink.connector.clickhouse.table.internal.executor;

import java.io.IOException;

public class BufferMemoryLimiter {
    // write-local 场景下多个 shard executor 共享同一份内存预算，避免各自把缓冲顶满。
    private final long maxBytes;
    private long usedBytes;

    public BufferMemoryLimiter(long maxBytes) {
        this.maxBytes = Math.max(1L, maxBytes);
    }

    public synchronized void acquire(long bytes) throws IOException {
        long requiredBytes = Math.max(0L, bytes);
        while (this.usedBytes + requiredBytes > this.maxBytes) {
            try {
                this.wait(200L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for shared buffer memory", e);
            }
        }
        this.usedBytes += requiredBytes;
    }

    public synchronized void release(long bytes) {
        this.usedBytes = Math.max(0L, this.usedBytes - Math.max(0L, bytes));
        this.notifyAll();
    }

    public synchronized long getUsedBytes() {
        return this.usedBytes;
    }

    public long getMaxBytes() {
        return this.maxBytes;
    }
}
