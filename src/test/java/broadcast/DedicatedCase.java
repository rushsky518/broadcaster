package broadcast;

import io.github.rushsky518.broadcast.KafkaDispatcher;
import io.github.rushsky518.broadcast.KafkaPublisher;
import io.github.rushsky518.broadcast.MessageListener;
import io.github.rushsky518.broadcast.Notify;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DedicatedCase {
    public static void main(String[] args) {
        KafkaPublisher publisher = new KafkaPublisher("127.0.0.1:9092");

        KafkaDispatcher kafkaDispatcher = new KafkaDispatcher("127.0.0.1:9092", "zhang", "ms_bus");
        ExecutorService pool = Executors.newFixedThreadPool(2);
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

        publisher.publish("ms_bus", "payment", "1");
        publisher.publish("ms_bus", "delivery", "2");

        kafkaDispatcher.shutdown();
    }
}
