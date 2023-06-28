package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class ResetConsumer {
    public static final String RESET_GROUP_ID = "reset-example-consumers";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, RESET_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("test"));

        KafkaPollThread<String, String> pollThread = new KafkaPollThread<>(consumer, () -> new KafkaTask<String, String>() {
            @Override
            public void accept(ConsumerRecord<String, String> record) {
                System.out.printf("thread:%s offset=%d, key=%s, value=%s\n", Thread.currentThread(),
                        record.offset(), record.key(), record.value());
            }
        }, "biz-poll-thread");

        pollThread.start();

        // start a ResetManager
        ResetManager resetManager = ResetManager.getResetManager("127.0.0.1:9092", "127.0.0.1", 8080);
        resetManager.register(pollThread);
    }
}
