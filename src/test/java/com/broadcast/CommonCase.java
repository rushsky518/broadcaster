package com.broadcast;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

public class CommonCase {
    public static void main(String[] args) {
        KafkaPublisher publisher = new KafkaPublisher("127.0.0.1:9092");

        KafkaDispatcher kafkaDispatcher = new KafkaDispatcher("127.0.0.1:9092", "zhang");

        final ExecutorService pool = Executors.newSingleThreadExecutor();
        kafkaDispatcher.subscribe("payment", new MessageListener<Notify>() {
            @Override
            public void onMessage(CharSequence channel, Notify msg) {
                // suggest async
                System.out.println(msg.getPayload());
            }
        }, pool);
        kafkaDispatcher.subscribe("delivery", new MessageListener<Notify>() {
            @Override
            public void onMessage(CharSequence channel, Notify msg) {
                // suggest async
                System.out.println(msg.getPayload());
            }
        }, pool);

        publisher.publish("payment", "1");
        publisher.publish("delivery", "2");

        LockSupport.park();
    }
}
