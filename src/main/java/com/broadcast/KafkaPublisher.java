package com.broadcast;

import brave.kafka.clients.KafkaTracing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.broadcast.KafkaDispatcher.DEFAULT_BROADCAST_BUS;

public class KafkaPublisher implements Publisher {
    private KafkaProducer<String, Notify> kafkaProducer;
    private Producer<String, Notify> tracedProducer;

    public KafkaPublisher(String bootstrap) {
        Properties properties = initProperties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public KafkaPublisher(String bootstrap, KafkaTracing kafkaTracing) {
        Properties properties = initProperties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        this.kafkaProducer = new KafkaProducer<>(properties);
        this.tracedProducer = kafkaTracing.producer(this.kafkaProducer);
    }

    private Properties initProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class.getName());
        return properties;
    }

    @Override
    public void publish(String channel, Object payload) {
        publish(DEFAULT_BROADCAST_BUS, channel, payload);
    }

    @Override
    public void publish(String topic, String channel, Object payload) {
        Notify notify = new Notify();
        notify.tag = channel;
        notify.payload = payload;

        ProducerRecord<String, Notify> producerRecord = new ProducerRecord<>(topic, 0, null, notify);
        if (this.tracedProducer != null) {
            tracedProducer.send(producerRecord);
        } else {
            this.kafkaProducer.send(producerRecord);
        }
    }
}
