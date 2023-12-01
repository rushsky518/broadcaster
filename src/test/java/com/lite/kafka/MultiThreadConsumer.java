package com.lite.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class MultiThreadConsumer {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.101.188:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "your-group-id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("logs"));

        MultiThreadGroup threadGroup = new MultiThreadGroup(4);

        KafkaPollThread<String, String> pollThread = new KafkaPollThread<>(consumer, new TaskGenerator<String, String>() {
            @Override
            public KafkaTask<String, String> generate() {
                return new KafkaTask<String, String>() {
                    @Override
                    public void accept(ConsumerRecord<String, String> re) {
                        System.out.printf("thread:%s offset=%d, key=%s, value=%s\n", Thread.currentThread(),
                                record.offset(), record.key(), record.value());
                    }
                };
            }
        }, "biz-poll-thread", threadGroup);

        pollThread.start();
    }
}
