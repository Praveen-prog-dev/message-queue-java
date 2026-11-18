package com.praveen.mq;

public record SendResult(String topic, int partition, long offset) {
}
