package io.github.rushsky518.broadcast;

import java.util.concurrent.ExecutorService;

public interface Subscriber {
    default void subscribe(String channel, MessageListener<Notify> listener) {}

    void subscribe(String channel, MessageListener<Notify> listener, ExecutorService executorService);
}
