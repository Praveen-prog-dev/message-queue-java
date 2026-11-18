package com.praveen.mq.broker;

import com.praveen.mq.SendResult;
import com.praveen.mq.TopicPartition;
import com.praveen.mq.client.Consumer;
import com.praveen.mq.client.Producer;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class Broker {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final GroupCoordinator coordinator = new GroupCoordinator(this);
    private final Clock clock;
    private final AtomicInteger consumerIds = new AtomicInteger();

    // consumers blocked in poll() wait here until something is appended anywhere
    private final ReentrantLock appendLock = new ReentrantLock();
    private final Condition appended = appendLock.newCondition();

    public Broker() {
        this(Clock.systemUTC());
    }

    public Broker(Clock clock) {
        this.clock = clock;
    }

    public void createTopic(String name, int partitions) {
        createTopic(name, partitions, 100_000);
    }

    public void createTopic(String name, int partitions, int retentionPerPartition) {
        if (partitions <= 0 || retentionPerPartition <= 0) {
            throw new IllegalArgumentException("partitions and retention must be positive");
        }
        if (topics.putIfAbsent(name, new Topic(name, partitions, retentionPerPartition)) != null) {
            throw new IllegalArgumentException("topic " + name + " already exists");
        }
    }

    public Producer producer() {
        return new Producer(this);
    }

    public Consumer consumer(String groupId) {
        return new Consumer(this, groupId, groupId + "-consumer-" + consumerIds.incrementAndGet());
    }

    public SendResult append(String topicName, String key, String payload) {
        Topic topic = topic(topicName);
        int partition = topic.partitionFor(key);
        long offset = topic.partition(partition).append(key, payload, clock.millis());

        appendLock.lock();
        try {
            appended.signalAll();
        } finally {
            appendLock.unlock();
        }
        return new SendResult(topicName, partition, offset);
    }

    public Topic topic(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            throw new IllegalArgumentException("unknown topic " + name);
        }
        return topic;
    }

    public PartitionLog log(TopicPartition tp) {
        return topic(tp.topic()).partition(tp.partition());
    }

    /** Returns false if the timeout passed without any new message being appended. */
    public boolean awaitAppend(long timeoutNanos) throws InterruptedException {
        appendLock.lock();
        try {
            return appended.await(timeoutNanos, TimeUnit.NANOSECONDS);
        } finally {
            appendLock.unlock();
        }
    }

    // group operations are exposed to the client package through these thin methods

    public int joinGroup(String groupId, String memberId, Set<String> topics) {
        topics.forEach(this::topic); // fail fast on typos
        return coordinator.join(groupId, memberId, topics);
    }

    public void leaveGroup(String groupId, String memberId) {
        coordinator.leave(groupId, memberId);
    }

    public int generation(String groupId, String memberId) {
        return coordinator.assignmentFor(groupId, memberId).generation();
    }

    public List<TopicPartition> assignment(String groupId, String memberId) {
        return coordinator.assignmentFor(groupId, memberId).partitions();
    }

    public void commit(String groupId, int generation, Map<TopicPartition, Long> offsets) {
        coordinator.commit(groupId, generation, offsets);
    }

    public Optional<Long> committedOffset(String groupId, TopicPartition tp) {
        return coordinator.committedOffset(groupId, tp);
    }
}
