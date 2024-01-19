# broadcaster

基于 kafka 实现事件广播的工具：
KafkaPublisher 向 topic(broadcast_bus) 发布通知，设置不同 channel 区分业务；

通过 KafkaDispatcher 订阅通知，设置 MessageListener 以及执行线程池。

对于通知数量较大的业务，需要单独指定广播的 topic，publisher 和 subscriber 统一使用该 topic。

已初步加入调用链 trace 信息。
```java
// KafkaPublisher 和 KafkaDispatcher 在进程中保持单例

// 发布消息
String bootstrapServer = xx;
KafkaPublisher publisher = new KafkaPublisher(bootstrapServer);
// publish 方法有三个参数：topic, channel, payload；channel 用于隔离业务，如果通知数据量过多，考虑指定额外的 topic
// 不指定 topic，则使用默认的 topic
publisher.publish("payment", "1");


// 订阅消息
// KafkaDispatcher 需要指定唯一的 group.id，建议使用 ip:port 的形式
KafkaDispatcher kafkaDispatcher = new KafkaDispatcher(bootstrapServer, uniqueGroupId);

// 通过 KafkaDispatcher 实现订阅逻辑
// 如果不指定线程池，则使用默认
final ExecutorService pool = Executors.newSingleThreadExecutor();
kafkaDispatcher.subscribe("payment", new MessageListener<Notify>() {
    @Override
    public void onMessage(CharSequence channel, Notify msg) {
        System.out.println(msg.getPayload());
    }
}, pool);
```
