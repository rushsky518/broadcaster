package io.github.rushsky518.broadcast;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JsonDeserializer<T> implements Deserializer<T> {
    @Override
    public T deserialize(String topic, byte[] data) {
        try {
           return (T) JsonJacksonCodec.INSTANCE.mapObjectMapper.readValue(data, Object.class);
        } catch (IOException e) {
            throw new SerializationException("Error when de-serializing", e);
        }
    }
}
