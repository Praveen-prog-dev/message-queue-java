package com.praveen.mq.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {

    private final String name;
    private final List<PartitionLog> partitions;
    private final AtomicInteger roundRobin = new AtomicInteger();

    Topic(String name, int partitionCount, int retentionPerPartition) {
        this.name = name;
        List<PartitionLog> logs = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            logs.add(new PartitionLog(name, i, retentionPerPartition));
        }
        this.partitions = List.copyOf(logs);
    }

    /**
     * Same key -> same partition, which is what gives per-key ordering (all events for
     * order-17 are consumed in the order they were produced). Messages without a key are
     * spread round-robin.
     */
    int partitionFor(String key) {
        if (key == null) {
            return Math.floorMod(roundRobin.getAndIncrement(), partitions.size());
        }
        return Math.floorMod(key.hashCode(), partitions.size());
    }

    public String name() {
        return name;
    }

    public int partitionCount() {
        return partitions.size();
    }

    public PartitionLog partition(int index) {
        return partitions.get(index);
    }
}
