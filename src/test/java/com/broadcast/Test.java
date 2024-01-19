package com.broadcast;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Test {
    static class Demo {
        String foo;
        String bar;
    }
    public static void main(String[] args) throws JsonProcessingException {
        Demo demo = new Demo();
        demo.foo = "foo";
        demo.bar = "bar";
        ObjectMapper om = JsonJacksonCodec.INSTANCE.getMapObjectMapper();
        String str = om.writeValueAsString(demo);

        Demo demo1 = om.readValue(str, Demo.class);
        System.out.println(demo1);
    }
}
