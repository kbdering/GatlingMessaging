package io.github.kbdering.kafka;

import io.github.kbdering.kafka.proto.DummyRequest;
import io.github.kbdering.kafka.util.SerializationHelper;
import io.github.kbdering.kafka.util.SerializationType;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class SerializationHelperTest {

    @Test
    public void testSerializeString() {
        String input = "test-string";
        byte[] result = SerializationHelper.serialize(input, SerializationType.STRING);
        assertNotNull(result);
        assertEquals(input, new String(result, StandardCharsets.UTF_8));
    }

    @Test
    public void testDeserializeString() {
        String input = "test-string";
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        String result = SerializationHelper.deserialize(bytes, SerializationType.STRING, String.class);
        assertEquals(input, result);
    }

    @Test
    public void testSerializeProtobuf() {
        DummyRequest request = DummyRequest.newBuilder().setRequestId("123").setRequestPayload("payload").build();
        byte[] result = SerializationHelper.serialize(request, SerializationType.PROTOBUF);
        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    public void testDeserializeProtobuf() {
        DummyRequest request = DummyRequest.newBuilder().setRequestId("123").setRequestPayload("payload").build();
        byte[] bytes = request.toByteArray();
        DummyRequest result = SerializationHelper.deserialize(bytes, SerializationType.PROTOBUF, DummyRequest.class);
        assertEquals(request.getRequestId(), result.getRequestId());
        assertEquals(request.getRequestPayload(), result.getRequestPayload());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSerializeMismatchType() {
        SerializationHelper.serialize("string", SerializationType.PROTOBUF);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeMismatchType() {
        byte[] bytes = "test".getBytes();
        SerializationHelper.deserialize(bytes, SerializationType.STRING, Integer.class);
    }
}
