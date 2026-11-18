package com.praveen.mq;

import com.praveen.mq.broker.Broker;
import com.praveen.mq.client.Consumer;
import com.praveen.mq.client.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerTest {

    private Broker broker;
    private Producer producer;

    @BeforeEach
    void setUp() {
        broker = new Broker();
        broker.createTopic("events", 3);
        producer = broker.producer();
    }

    @Test
    void sameKeyAlwaysGoesToSamePartitionInOrder() {
        List<SendResult> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            results.add(producer.send("events", "order-7", "step-" + i));
        }

        int partition = results.get(0).partition();
        for (int i = 0; i < results.size(); i++) {
            assertEquals(partition, results.get(i).partition());
            assertEquals(i, results.get(i).offset());
        }
    }

    @Test
    void consumerReadsMessagesInProduceOrderPerKey() {
        for (int i = 0; i < 20; i++) {
            producer.send("events", "k" + (i % 4), String.valueOf(i));
        }
        Consumer consumer = broker.consumer("g");
        consumer.subscribe("events");

        List<Message> all = drain(consumer);
        assertEquals(20, all.size());
        for (String key : List.of("k0", "k1", "k2", "k3")) {
            List<Integer> seen = all.stream().filter(m -> m.key().equals(key))
                    .map(m -> Integer.parseInt(m.payload())).toList();
            assertEquals(seen.stream().sorted().toList(), seen, "out of order for " + key);
        }
    }

    @Test
    void consumerGroupSplitsPartitionsWithoutOverlap() {
        Consumer a = broker.consumer("billing");
        Consumer b = broker.consumer("billing");
        a.subscribe("events");
        b.subscribe("events");

        Set<TopicPartition> union = new HashSet<>(a.assignment());
        union.addAll(b.assignment());

        assertEquals(3, union.size());
        assertEquals(3, a.assignment().size() + b.assignment().size());
    }

    @Test
    void eachMessageGoesToExactlyOneMemberOfTheGroup() {
        Consumer a = broker.consumer("billing");
        Consumer b = broker.consumer("billing");
        a.subscribe("events");
        b.subscribe("events");
        for (int i = 0; i < 300; i++) {
            producer.send("events", "key-" + i, "m" + i);
        }

        List<Message> fromA = drain(a);
        List<Message> fromB = drain(b);
        Set<String> payloads = new HashSet<>();
        fromA.forEach(m -> payloads.add(m.payload()));
        fromB.forEach(m -> payloads.add(m.payload()));

        assertEquals(300, fromA.size() + fromB.size());
        assertEquals(300, payloads.size());
    }

    @Test
    void differentGroupsEachGetEveryMessage() {
        for (int i = 0; i < 50; i++) {
            producer.send("events", "m" + i);
        }
        Consumer billing = broker.consumer("billing");
        Consumer analytics = broker.consumer("analytics");
        billing.subscribe("events");
        analytics.subscribe("events");

        assertEquals(50, drain(billing).size());
        assertEquals(50, drain(analytics).size());
    }

    @Test
    void committedOffsetsSurviveConsumerRestart() {
        for (int i = 0; i < 10; i++) {
            producer.send("events", "k", "m" + i);
        }
        Consumer first = broker.consumer("g");
        first.subscribe("events");
        assertEquals(10, drain(first).size());
        first.commitSync();
        first.close();

        producer.send("events", "k", "m10");
        Consumer second = broker.consumer("g");
        second.subscribe("events");

        List<Message> rest = drain(second);
        assertEquals(1, rest.size());
        assertEquals("m10", rest.get(0).payload());
    }

    @Test
    void uncommittedMessagesAreRedeliveredAfterRebalance() {
        for (int i = 0; i < 10; i++) {
            producer.send("events", "k", "m" + i);
        }
        Consumer crashed = broker.consumer("g");
        crashed.subscribe("events");
        assertEquals(10, drain(crashed).size());
        crashed.close(); // processed but never committed

        Consumer replacement = broker.consumer("g");
        replacement.subscribe("events");
        assertEquals(10, drain(replacement).size(), "at-least-once: work is redone");
    }

    @Test
    void staleMemberCannotCommitAfterRebalance() {
        Consumer a = broker.consumer("g");
        a.subscribe("events");
        producer.send("events", "x");
        drain(a);

        Consumer b = broker.consumer("g");
        b.subscribe("events"); // bumps the generation; a hasn't polled since

        assertThrows(IllegalStateException.class, a::commitSync);
    }

    @Test
    void extraConsumersBeyondPartitionCountSitIdle() {
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Consumer c = broker.consumer("g");
            c.subscribe("events");
            consumers.add(c);
        }
        long idle = consumers.stream().filter(c -> c.assignment().isEmpty()).count();
        assertEquals(2, idle);
    }

    @Test
    void retentionDropsOldMessagesAndConsumerStartsAtEarliestRetained() {
        broker.createTopic("logs", 1, 100);
        for (int i = 0; i < 250; i++) {
            producer.send("logs", "line-" + i);
        }
        Consumer consumer = broker.consumer("reader");
        consumer.subscribe("logs");

        List<Message> retained = drain(consumer);
        assertTrue(retained.size() <= 111, "kept " + retained.size());
        assertEquals("line-249", retained.get(retained.size() - 1).payload());
        assertEquals(retained.get(0).offset(), broker.topic("logs").partition(0).logStartOffset());
    }

    @Test
    void pollBlocksUntilDataArrives() throws InterruptedException {
        Consumer consumer = broker.consumer("g");
        consumer.subscribe("events");
        AtomicReference<List<Message>> received = new AtomicReference<>();

        Thread poller = new Thread(() -> received.set(consumer.poll(Duration.ofSeconds(3))));
        long start = System.nanoTime();
        poller.start();
        Thread.sleep(150);
        producer.send("events", "late", "hello");
        poller.join();
        long tookMillis = (System.nanoTime() - start) / 1_000_000;

        assertEquals(1, received.get().size());
        assertTrue(tookMillis < 1_000, "poll should return soon after the send, took " + tookMillis + "ms");
    }

    @Test
    void unknownTopicFailsFast() {
        assertThrows(IllegalArgumentException.class, () -> producer.send("nope", "x"));
        assertThrows(IllegalArgumentException.class, () -> broker.consumer("g").subscribe("nope"));
    }

    private static List<Message> drain(Consumer consumer) {
        List<Message> all = new ArrayList<>();
        while (true) {
            List<Message> batch = consumer.poll(Duration.ZERO);
            if (batch.isEmpty()) {
                return all;
            }
            all.addAll(batch);
        }
    }
}
