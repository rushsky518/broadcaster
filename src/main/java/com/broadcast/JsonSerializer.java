package com.broadcast;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer implements Serializer<Notify> {
    @Override
    public byte[] serialize(String topic, Notify notify) {
        if (notify == null) {
            return null;
        }

        try {
            String data = JsonJacksonCodec.INSTANCE.mapObjectMapper.writeValueAsString(notify.getPayload());
            notify.payload = null;
            notify.data = data;
            return JsonJacksonCodec.INSTANCE.mapObjectMapper.writeValueAsBytes(notify);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error when serializing Object to byte[]", e);
        }
    }
}
