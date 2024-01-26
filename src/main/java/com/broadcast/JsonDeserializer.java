package com.broadcast;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonDeserializer implements Deserializer<Notify> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonDeserializer.class);

    @Override
    public Notify deserialize(String topic, byte[] data) {
        try {
           return JsonJacksonCodec.INSTANCE.mapObjectMapper.readValue(data, Notify.class);
        } catch (IOException e) {
            LOGGER.error("error deserialize", e);
        }

        Notify fallback = new Notify();
        fallback.data = new String(data);
        return fallback;
    }
}
