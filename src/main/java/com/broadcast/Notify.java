package com.broadcast;

public class Notify {
    long createTime;
    String tag;
    // 应用中使用到的 java 类型
    Object payload;
    // kafka 中存储的字符串
    String data;

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
