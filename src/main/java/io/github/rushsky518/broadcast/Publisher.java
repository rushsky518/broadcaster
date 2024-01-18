package io.github.rushsky518.broadcast;

public interface Publisher {
    void publish(String topic, Object payload);
    default void publish(String topic, String channel, Object payload) {}
}
