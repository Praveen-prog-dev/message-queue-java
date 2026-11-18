package com.praveen.mq.broker;

import com.praveen.mq.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Tracks consumer group membership, partition assignments and committed offsets.
 *
 * Whenever a member joins or leaves, the group's generation is bumped and partitions are
 * reassigned. Consumers notice the new generation on their next poll and restart from the
 * committed offsets, so anything processed but not committed gets delivered again
 * (at-least-once).
 */
class GroupCoordinator {

    private final Broker broker;
    private final Map<String, Group> groups = new HashMap<>();

    GroupCoordinator(Broker broker) {
        this.broker = broker;
    }

    synchronized int join(String groupId, String memberId, Set<String> topics) {
        Group group = groups.computeIfAbsent(groupId, id -> new Group());
        group.subscriptions.put(memberId, Set.copyOf(topics));
        return group.rebalance(broker);
    }

    synchronized void leave(String groupId, String memberId) {
        Group group = groups.get(groupId);
        if (group != null && group.subscriptions.remove(memberId) != null) {
            group.rebalance(broker);
        }
    }

    synchronized Assignment assignmentFor(String groupId, String memberId) {
        Group group = groups.get(groupId);
        if (group == null) {
            return new Assignment(0, List.of());
        }
        return new Assignment(group.generation, group.assignments.getOrDefault(memberId, List.of()));
    }

    synchronized void commit(String groupId, int generation, Map<TopicPartition, Long> offsets) {
        Group group = groups.get(groupId);
        if (group == null || group.generation != generation) {
            // a zombie consumer from an old generation must not overwrite offsets
            // of partitions that now belong to someone else
            throw new IllegalStateException("commit rejected: group " + groupId + " has rebalanced");
        }
        group.committed.putAll(offsets);
    }

    synchronized Optional<Long> committedOffset(String groupId, TopicPartition tp) {
        Group group = groups.get(groupId);
        return group == null ? Optional.empty() : Optional.ofNullable(group.committed.get(tp));
    }

    record Assignment(int generation, List<TopicPartition> partitions) {
    }

    private static final class Group {
        int generation;
        final Map<String, Set<String>> subscriptions = new TreeMap<>();
        final Map<String, List<TopicPartition>> assignments = new HashMap<>();
        final Map<TopicPartition, Long> committed = new HashMap<>();

        /**
         * Round-robin assignment: partitions are sorted and dealt out to the members that
         * subscribe to their topic, like dealing cards. With more members than partitions
         * some members get nothing - they're standbys.
         */
        int rebalance(Broker broker) {
            generation++;
            assignments.clear();
            subscriptions.keySet().forEach(m -> assignments.put(m, new ArrayList<>()));

            Set<String> allTopics = new TreeSet<>();
            subscriptions.values().forEach(allTopics::addAll);

            for (String topicName : allTopics) {
                List<String> members = subscriptions.entrySet().stream()
                        .filter(e -> e.getValue().contains(topicName))
                        .map(Map.Entry::getKey)
                        .toList();
                Topic topic = broker.topic(topicName);
                for (int p = 0; p < topic.partitionCount(); p++) {
                    String owner = members.get(p % members.size());
                    assignments.get(owner).add(new TopicPartition(topicName, p));
                }
            }
            return generation;
        }
    }
}
