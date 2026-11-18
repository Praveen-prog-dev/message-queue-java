package com.praveen.mq;

import java.util.Comparator;

public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {

    private static final Comparator<TopicPartition> ORDER =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    @Override
    public int compareTo(TopicPartition other) {
        return ORDER.compare(this, other);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
