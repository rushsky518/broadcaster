package io.github.rushsky518.broadcast;

public class Notify {
    long createTime;
    String tag;
    Object payload;

    public long getCreateTime() {
        return createTime;
    }

    public String getTag() {
        return tag;
    }

    public Object getPayload() {
        return payload;
    }
}
