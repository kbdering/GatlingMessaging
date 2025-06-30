package io.github.kbdering.kafka.javaapi;

import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.*;
import io.gatling.javaapi.core.internal.Expressions;
import io.github.kbdering.kafka.MessageCheck;
import io.github.kbdering.kafka.SerializationType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import io.github.kbdering.kafka.KafkaRequestReplyActionBuilder; //Import to avoid long class name
import io.github.kbdering.kafka.KafkaActionBuilder;//Import to avoid long class name

public class KafkaDsl {

    public static KafkaProtocolBuilder kafka() {
        return new KafkaProtocolBuilder();
    }
    /**
     * Creates a KafkaActionBuilder for sending messages to a Kafka topic.  Uses Expressions
     * for key and value to allow dynamic values from the Gatling session.
     *
     * @param topic The target Kafka topic.
     * @param key   An expression string that resolves to the message key.
     * @param value An expression string that resolves to the message value.
     * @return A KafkaActionBuilder for sending messages.
     */
    public static KafkaActionBuilder kafka(String topic, String key, String value) {
        return new KafkaActionBuilder(topic, (Function<Session, String>) Expressions.toStringExpression(key), (Function<Session, String>) Expressions.toStringExpression(value), null);
    }

    /**
     * Creates a KafkaActionBuilder for sending messages to a Kafka topic.  Uses Expressions
     * for key and value to allow dynamic values from the Gatling session.
     *
     * @param topic         The target Kafka topic.
     * @param key           An expression string that resolves to the message key.
     * @param value         An expression string that resolves to the message value.
     * @param waitForAck    boolean that defines if it is necessary to wait for acknowledgement.
     * @param timeout       timeout value.
     * @param timeUnit      timeout unit.
     * @return A KafkaActionBuilder for sending messages.
     */

    public static KafkaActionBuilder kafka(String topic, String key, String value, boolean waitForAck, long timeout, TimeUnit timeUnit) {
        return new KafkaActionBuilder(topic, (Function<Session, String>) Expressions.toStringExpression(key), (Function<Session, String>) Expressions.toStringExpression(value), null, waitForAck, timeout, timeUnit);
    }


    /**
     * Creates a KafkaActionBuilder for sending messages, using lambdas (Session -> String)
     * for dynamic keys and values.
     *
     * @param topic        The target Kafka topic.
     * @param keyFunction  A function that takes a Gatling Session and returns the message key.
     * @param valueFunction A function that takes a Gatling Session and returns the message value.
     * @return A KafkaActionBuilder.
     */
    public static KafkaActionBuilder kafka(String topic, java.util.function.Function<Session, String> keyFunction, java.util.function.Function<Session, String> valueFunction) {
        return new KafkaActionBuilder(topic, keyFunction, valueFunction, null);
    }

    /**
     * Creates a KafkaActionBuilder for sending messages, using lambdas (Session -> String)
     * for dynamic keys and values.
     *
     * @param topic        The target Kafka topic.
     * @param keyFunction  A function that takes a Gatling Session and returns the message key.
     * @param valueFunction A function that takes a Gatling Session and returns the message value.
     * @param waitForAck    boolean that defines if it is necessary to wait for acknowledgement.
     * @param timeout       timeout value.
     * @param timeUnit      timeout unit.
     * @return A KafkaActionBuilder.
     */
    public static KafkaActionBuilder kafka(String topic, java.util.function.Function<Session, String> keyFunction, java.util.function.Function<Session, String> valueFunction,
                                           boolean waitForAck, long timeout, TimeUnit timeUnit) {
        return new KafkaActionBuilder(topic, keyFunction, valueFunction, null, waitForAck, timeout, timeUnit);
    }


    // Existing method, adapted to use byte[] for String value, assuming STRING serialization and no checks by default
    public static KafkaRequestReplyActionBuilder kafkaRequestReply(String requestTopic, String responseTopic,
                                                                   Function<Session, String> keyFunction,
                                                                   String valueExpression, // Gatling EL for string value
                                                                   Protocol protocol,
                                                                   long timeout, TimeUnit timeUnit) {
        Function<Session, String> stringValueFunction = (Function<Session, String>) Expressions.toStringExpression(valueExpression);
        Function<Session, byte[]> byteValueFunction = session -> stringValueFunction.apply(session).getBytes(StandardCharsets.UTF_8);
        return new KafkaRequestReplyActionBuilder(requestTopic, responseTopic, keyFunction, byteValueFunction, SerializationType.STRING, protocol, Collections.emptyList(), timeout, timeUnit);
    }

    // Recommended: Overloaded method without the Protocol parameter
    public static KafkaRequestReplyActionBuilder kafkaRequestReply(String requestTopic, String responseTopic,
                                                                   Function<Session, String> keyFunction,
                                                                   Function<Session, byte[]> valueFunction,
                                                                   SerializationType requestSerializationType,
                                                                   List<MessageCheck<?, ?>> messageChecks,
                                                                   long timeout, TimeUnit timeUnit) {
        // Pass null for the protocol; the ActionBuilder will resolve it from the context.
        return new KafkaRequestReplyActionBuilder(requestTopic, responseTopic, keyFunction, valueFunction, requestSerializationType, null, messageChecks, timeout, timeUnit);
    }

    // New comprehensive method for byte[] value, explicit SerializationType, and MessageChecks
    public static KafkaRequestReplyActionBuilder kafkaRequestReply(String requestTopic, String responseTopic,
                                                                   Function<Session, String> keyFunction,
                                                                   Function<Session, byte[]> valueFunction,
                                                                   SerializationType requestSerializationType,
                                                                   List<MessageCheck<?, ?>> messageChecks, // Changed to generic wildcard
                                                                   Protocol protocol,
                                                                   long timeout, TimeUnit timeUnit) {
        return new KafkaRequestReplyActionBuilder(requestTopic, responseTopic, keyFunction, valueFunction, requestSerializationType, protocol, messageChecks, timeout, timeUnit);
    }
}