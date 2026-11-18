package com.praveen.mq.client;

import com.praveen.mq.SendResult;
import com.praveen.mq.broker.Broker;

public class Producer {

    private final Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }

    public SendResult send(String topic, String key, String payload) {
        return broker.append(topic, key, payload);
    }

    public SendResult send(String topic, String payload) {
        return send(topic, null, payload);
    }
}
