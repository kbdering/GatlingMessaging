package io.github.kbdering.kafka.util;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.google.protobuf.Message; // Import the base Protobuf Message interface
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.BinaryDecoder;

public class SerializationHelper {

    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] bytes, SerializationType type, Class<T> targetClass) {
        if (bytes == null) {
            return null;
        }

        switch (type) {
            case STRING:
                if (targetClass == String.class) {
                    return (T) new String(bytes, StandardCharsets.UTF_8);
                } else {
                    throw new IllegalArgumentException(
                            "Mismatched target class for STRING deserialization. Expected String, got "
                                    + targetClass.getName());
                }
            case PROTOBUF: // NOSONAR
                try {
                    // Every Protobuf generated class has a static parseFrom(byte[]) method
                    Method parseFromMethod = targetClass.getMethod("parseFrom", byte[].class);
                    return (T) parseFromMethod.invoke(null, (Object) bytes);
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    // InvocationTargetException can wrap InvalidProtocolBufferException
                    throw new RuntimeException(
                            "Protobuf deserialization error for " + targetClass.getName() + ": " + e.getMessage(), e);
                }
            case AVRO:
                try {
                    SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(targetClass);
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
                    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
                    return datumReader.read(null, decoder);
                } catch (IOException e) {
                    throw new RuntimeException(
                            "Avro deserialization error for " + targetClass.getName() + ": " + e.getMessage(), e);
                } catch (Exception e) { // Catch other potential reflection/instantiation issues if schema was fetched
                                        // via instance
                    throw new RuntimeException("Error during Avro deserialization setup for " + targetClass.getName()
                            + ": " + e.getMessage(), e);
                }

            default:
                throw new IllegalArgumentException("Unsupported serialization type: " + type);
        }
    }

    public static byte[] serialize(Object object, SerializationType type) {
        if (object == null) {
            return null;
        }

        switch (type) {
            case STRING:
                return object.toString().getBytes(StandardCharsets.UTF_8);
            case PROTOBUF:
                if (object instanceof Message) {
                    return ((Message) object).toByteArray();
                }
                throw new IllegalArgumentException("Object is not a Protobuf Message type for PROTOBUF serialization: "
                        + object.getClass().getName());
            case AVRO:
                if (object instanceof SpecificRecordBase) {
                    SpecificRecordBase record = (SpecificRecordBase) object;
                    SpecificDatumWriter<SpecificRecordBase> datumWriter = new SpecificDatumWriter<>(record.getSchema());
                    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                        datumWriter.write(record, encoder);
                        encoder.flush();
                        return outputStream.toByteArray();
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Avro serialization error for " + object.getClass().getName() + ": " + e.getMessage(),
                                e);
                    }
                }
                throw new IllegalArgumentException(
                        "Object is not an Avro SpecificRecordBase type for AVRO serialization: "
                                + object.getClass().getName());
            default:
                throw new IllegalArgumentException("Unsupported serialization type for object: " + type + " for object "
                        + object.getClass().getName());
        }
    }
}