package com.praveen.mq;

public record Message(String topic, int partition, long offset, String key, String payload, long timestamp) {

    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}
