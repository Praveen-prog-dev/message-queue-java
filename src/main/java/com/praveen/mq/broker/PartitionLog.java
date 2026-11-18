package com.praveen.mq.broker;

import com.praveen.mq.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Append-only log for one partition. Offsets are absolute and never reused: after old
 * messages are removed by retention, {@code logStartOffset} moves forward but the offsets of
 * the remaining messages don't change - consumers rely on that.
 */
public class PartitionLog {

    private final String topic;
    private final int partition;
    private final int retentionLimit;
    private final List<Message> messages = new ArrayList<>();
    private long logStartOffset = 0;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    PartitionLog(String topic, int partition, int retentionLimit) {
        this.topic = topic;
        this.partition = partition;
        this.retentionLimit = retentionLimit;
    }

    long append(String key, String payload, long timestamp) {
        lock.writeLock().lock();
        try {
            long offset = logStartOffset + messages.size();
            messages.add(new Message(topic, partition, offset, key, payload, timestamp));
            applyRetention();
            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Up to {@code max} messages starting at {@code fromOffset}. Empty if there's nothing new. */
    public List<Message> read(long fromOffset, int max) {
        lock.readLock().lock();
        try {
            long start = Math.max(fromOffset, logStartOffset);
            int from = (int) (start - logStartOffset);
            if (from >= messages.size()) {
                return List.of();
            }
            int to = Math.min(messages.size(), from + max);
            return List.copyOf(messages.subList(from, to));
        } finally {
            lock.readLock().unlock();
        }
    }

    public long logStartOffset() {
        lock.readLock().lock();
        try {
            return logStartOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long endOffset() {
        lock.readLock().lock();
        try {
            return logStartOffset + messages.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void applyRetention() {
        // trim in chunks of 10% instead of on every append, removing from the
        // front of an ArrayList is O(n)
        int overflow = messages.size() - retentionLimit;
        if (overflow > Math.max(1, retentionLimit / 10)) {
            messages.subList(0, overflow).clear();
            logStartOffset += overflow;
        }
    }
}
