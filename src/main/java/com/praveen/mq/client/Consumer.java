package com.praveen.mq.client;

import com.praveen.mq.Message;
import com.praveen.mq.TopicPartition;
import com.praveen.mq.broker.Broker;
import com.praveen.mq.broker.PartitionLog;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A group member. Not thread-safe - like KafkaConsumer, each consumer is meant to be
 * owned by one thread. Scale out by adding consumers, not threads per consumer.
 */
public class Consumer implements AutoCloseable {

    private static final long MAX_WAIT_SLICE_NANOS = Duration.ofMillis(50).toNanos();

    private final Broker broker;
    private final String groupId;
    private final String memberId;
    private final Set<String> subscriptions = new HashSet<>();
    private final Map<TopicPartition, Long> positions = new HashMap<>();
    private int maxPollRecords = 100;
    private int knownGeneration = -1;
    private boolean closed;

    public Consumer(Broker broker, String groupId, String memberId) {
        this.broker = broker;
        this.groupId = groupId;
        this.memberId = memberId;
    }

    public void subscribe(String... topics) {
        ensureOpen();
        subscriptions.addAll(List.of(topics));
        broker.joinGroup(groupId, memberId, subscriptions);
    }

    public Consumer maxPollRecords(int max) {
        this.maxPollRecords = max;
        return this;
    }

    public List<Message> poll(Duration timeout) {
        ensureOpen();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            refreshAssignmentIfRebalanced();
            List<Message> batch = fetch();
            long remaining = deadline - System.nanoTime();
            if (!batch.isEmpty() || remaining <= 0) {
                return batch;
            }
            try {
                // capped wait: an append that lands between fetch() and await() would
                // otherwise be missed until the full timeout runs out
                broker.awaitAppend(Math.min(remaining, MAX_WAIT_SLICE_NANOS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return List.of();
            }
        }
    }

    /** Commits the position after the last message returned by poll(). */
    public void commitSync() {
        ensureOpen();
        broker.commit(groupId, knownGeneration, Map.copyOf(positions));
    }

    public List<TopicPartition> assignment() {
        refreshAssignmentIfRebalanced();
        return List.copyOf(positions.keySet()).stream().sorted().toList();
    }

    public String memberId() {
        return memberId;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            broker.leaveGroup(groupId, memberId);
        }
    }

    private List<Message> fetch() {
        List<Message> batch = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
            int room = maxPollRecords - batch.size();
            if (room <= 0) {
                break;
            }
            PartitionLog log = broker.log(entry.getKey());
            List<Message> messages = log.read(entry.getValue(), room);
            if (!messages.isEmpty()) {
                entry.setValue(messages.get(messages.size() - 1).offset() + 1);
                batch.addAll(messages);
            }
        }
        return batch;
    }

    private void refreshAssignmentIfRebalanced() {
        int generation = broker.generation(groupId, memberId);
        if (generation == knownGeneration) {
            return;
        }
        knownGeneration = generation;
        positions.clear();
        for (TopicPartition tp : broker.assignment(groupId, memberId)) {
            PartitionLog log = broker.log(tp);
            // no commit yet -> start from the earliest message still retained;
            // committed offset already deleted by retention -> same thing
            long start = broker.committedOffset(groupId, tp).orElse(log.logStartOffset());
            positions.put(tp, Math.max(start, log.logStartOffset()));
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("consumer is closed");
        }
    }
}
