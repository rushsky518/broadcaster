package com.broadcast;

import brave.Span;
import brave.Tracer;
import brave.kafka.clients.KafkaTracing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class KafkaDispatcher extends Thread implements Subscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDispatcher.class);
    public static final String DEFAULT_BROADCAST_BUS = "broadcast_bus";
    private KafkaConsumer<String, Notify> kafkaConsumer;
    private String topic;
    private boolean newest = true;
    private volatile boolean stop = false;
    private Map<String, MessageListener<Notify>> subscriberMap = new ConcurrentHashMap<>();
    private Map<String, ExecutorService> executorMap = new ConcurrentHashMap<>();
    private KafkaDispatcher kafkaDispatcher;
    private final ExecutorService defaultExecutorService = Executors.newSingleThreadExecutor();
    private KafkaTracing kafkaTracing;

    public KafkaDispatcher(String bootstrap, String uniqueGroupId) {
        Properties properties = initProperties(bootstrap, uniqueGroupId);
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.start();
    }
    public KafkaDispatcher(String bootstrap, String uniqueGroupId, KafkaTracing kafkaTracing) {
        Properties properties = initProperties(bootstrap, uniqueGroupId);
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.kafkaTracing = kafkaTracing;
        this.start();
    }

    public KafkaDispatcher(String bootstrap, String uniqueGroupId, String topic) {
        Properties properties = initProperties(bootstrap, uniqueGroupId);
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.topic = topic;
        this.start();
    }

    public KafkaDispatcher(String bootstrap, String uniqueGroupId, String topic, KafkaTracing kafkaTracing) {
        Properties properties = initProperties(bootstrap, uniqueGroupId);
        this.kafkaConsumer = new KafkaConsumer<>(properties);
        this.topic = topic;
        this.kafkaTracing = kafkaTracing;
        this.start();
    }

    private Properties initProperties(String bootstrap, String uniqueGroupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JsonDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    @Override
    public void subscribe(String channel, MessageListener<Notify> listener) {
        subscriberMap.put(channel, listener);
        executorMap.put(channel, defaultExecutorService);
    }

    @Override
    public void subscribe(String channel, MessageListener<Notify> listener, ExecutorService executorService) {
        subscriberMap.put(channel, listener);
        executorMap.put(channel, executorService);
    }

    @Override
    public void run() {
        TopicPartition tp = (topic != null) ? new TopicPartition(topic, 0) : new TopicPartition(DEFAULT_BROADCAST_BUS, 0);
        List<TopicPartition> tps = Collections.singletonList(tp);
        kafkaConsumer.assign(tps);
        if (newest) {
            kafkaConsumer.seekToEnd(tps);
        }

        final List<Future> futureList = new ArrayList<>(32);
        while (!stop) {
            ConsumerRecords<String, Notify> records;
            try {
                records = kafkaConsumer.poll(Duration.ofSeconds(5));
            } catch (Exception ex) {
                LOGGER.error("poll error", ex);
                continue;
            }

            for (ConsumerRecord<String, Notify> record : records) {
                Notify notify = record.value();
                notify.createTime = record.timestamp();
                String tag = notify.getTag();
                ExecutorService executorService = executorMap.getOrDefault(tag, defaultExecutorService);
                MessageListener<Notify> listener = subscriberMap.get(tag);
                if (listener == null) {
                    LOGGER.trace("{} listener is empty", tag);
                    continue;
                }
                Runnable runnable = () -> {
                    Span span = null;
                    if (this.kafkaTracing != null) {
                        span = this.kafkaTracing.nextSpan(record).name(this.getClass().getName()).start();
                        Tracer tracer = this.kafkaTracing.messagingTracing().tracing().tracer();
                        tracer.withSpanInScope(span);
                    }
                    try {
                        listener.onMessage(notify.tag, notify);
                    } catch (Exception ex) {
                        LOGGER.error("dispatch msg to {} error", notify.tag);
                        if (span != null) {
                            span.error(ex);
                        }
                    } finally {
                        if (span != null) {
                            span.finish();
                        }
                    }
                };

                Future<?> future = executorService.submit(runnable);
                futureList.add(future);
            }

            // 等待任务执行
            try {
                for (Future future : futureList) {
                    future.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("future get error", e);
            } finally {
                futureList.clear();
            }
            kafkaConsumer.commitSync();
        }

        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
    }

    public void shutdown() {
        stop = true;
        this.interrupt();
    }
}
