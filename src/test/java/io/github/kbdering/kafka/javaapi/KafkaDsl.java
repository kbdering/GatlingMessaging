package io.github.kbdering.kafka.javaapi;

import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.*;
import io.gatling.javaapi.core.internal.Expressions;

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


    public static KafkaRequestReplyActionBuilder kafkaRequestReply(String requestTopic, String responseTopic, java.util.function.Function<Session, String> keyFunction, String value,  Protocol protocol,  long timeout, TimeUnit timeUnit) {
        return new KafkaRequestReplyActionBuilder(requestTopic, responseTopic, keyFunction, session-> {return session.get(value);}, protocol, timeout, timeUnit);
    }
}