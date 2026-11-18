package com.praveen.mq;

import com.praveen.mq.broker.Broker;
import com.praveen.mq.client.Consumer;
import com.praveen.mq.client.Producer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class Demo {

    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        broker.createTopic("orders", 4);

        // two services consume the same topic independently:
        // billing scales out to 2 instances, analytics runs 1
        Map<String, AtomicInteger> processed = new ConcurrentHashMap<>();
        int total = 1_000;
        CountDownLatch billingDone = new CountDownLatch(total);
        CountDownLatch analyticsDone = new CountDownLatch(total);

        List<Thread> workers = List.of(
                startWorker(broker.consumer("billing"), processed, billingDone),
                startWorker(broker.consumer("billing"), processed, billingDone),
                startWorker(broker.consumer("analytics"), processed, analyticsDone));

        Thread.sleep(100); // let everyone join before producing
        Producer producer = broker.producer();
        for (int i = 0; i < total; i++) {
            String orderId = "order-" + (i % 50);
            producer.send("orders", orderId, "{\"orderId\":\"" + orderId + "\",\"seq\":" + i + "}");
        }

        billingDone.await();
        analyticsDone.await();
        workers.forEach(Thread::interrupt);

        System.out.println("Messages processed per consumer:");
        processed.forEach((consumer, count) -> System.out.println("  " + consumer + " -> " + count));
    }

    private static Thread startWorker(Consumer consumer, Map<String, AtomicInteger> processed, CountDownLatch done) {
        consumer.subscribe("orders");
        Thread t = new Thread(() -> {
            try (consumer) {
                while (!Thread.currentThread().isInterrupted()) {
                    List<Message> batch = consumer.poll(Duration.ofMillis(200));
                    for (Message m : batch) {
                        processed.computeIfAbsent(consumer.memberId(), id -> new AtomicInteger()).incrementAndGet();
                        done.countDown();
                    }
                    if (batch.isEmpty()) {
                        continue;
                    }
                    try {
                        consumer.commitSync();
                    } catch (IllegalStateException rebalanced) {
                        // partitions moved while we were processing; the new owner will
                        // re-read from the last commit
                        System.out.println(consumer.memberId() + ": " + rebalanced.getMessage());
                    }
                }
            }
        }, consumer.memberId());
        t.start();
        return t;
    }
}
