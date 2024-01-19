package com.broadcast;

public interface MessageListener<M> {

    /**
     * Invokes on every message in topic
     *
     * @param channel of topic
     * @param msg topic message
     */
    void onMessage(CharSequence channel, M msg);

}