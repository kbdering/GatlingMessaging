/*
 * Copyright 2026 Perfluencer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.perfluencer.kafka.javaapi;

import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.Session;
import pl.perfluencer.kafka.actions.KafkaActionBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Fluent builder for Kafka fire-and-forget send actions.
 *
 * <p>
 * Provides a Gatling-esque DSL for configuring fire-and-forget sends with
 * multiple serialization types:
 *
 * <pre>{@code
 * // String (default)
 * kafka("Send Order")
 *     .send()
 *     .topic("orders")
 *     .key(session -> "key")
 *     .value(session -> "{\"orderId\":\"123\"}")
 *
 * // Avro
 * kafka("Send Avro")
 *     .send()
 *     .topic("users")
 *     .key(session -> UUID.randomUUID().toString())
 *     .value(session -> createGenericRecord())
 *     .asAvro()
 *
 * // Bytes
 * kafka("Send Bytes")
 *     .send()
 *     .topic("binary-data")
 *     .key(session -> "key")
 *     .value(session -> serializeAvro(record))
 *     .asBytes()
 * }</pre>
 */
public class KafkaFireAndForget implements ActionBuilder {

    private final String requestName;
    private String topic;
    private Function<Session, String> keyFunction;
    private Function<Session, Object> valueFunction;
    private final Map<String, Function<Session, String>> headers = new HashMap<>();
    private boolean waitForAck = true;
    private long timeout = 30;
    private TimeUnit timeUnit = TimeUnit.SECONDS;

    public KafkaFireAndForget(String requestName) {
        this.requestName = requestName;
    }

    // ==================== TOPIC ====================

    /**
     * Sets the target Kafka topic.
     */
    public KafkaFireAndForget topic(String topic) {
        this.topic = topic;
        return this;
    }

    // ==================== KEY CONFIGURATION ====================

    /**
     * Sets the message key using a static value.
     */
    public KafkaFireAndForget key(String key) {
        this.keyFunction = KafkaDsl.toJavaFunction(key);
        return this;
    }

    /**
     * Sets the message key using a session function.
     */
    public KafkaFireAndForget key(Function<Session, String> keyFunction) {
        this.keyFunction = keyFunction;
        return this;
    }

    /**
     * Sets the message key using a Scala session function.
     */
    public KafkaFireAndForget keyScala(scala.Function1<io.gatling.core.session.Session, String> keyFunction) {
        this.keyFunction = session -> keyFunction.apply(session.asScala());
        return this;
    }

    // ==================== VALUE CONFIGURATION ====================

    /**
     * Sets the message value using a static string.
     */
    public KafkaFireAndForget value(String value) {
        Function<Session, String> stringFunction = KafkaDsl.toJavaFunction(value);
        this.valueFunction = session -> stringFunction.apply(session);
        return this;
    }

    /**
     * Sets the message value using a session function returning any Object
     * (String, byte[], GenericRecord, Protobuf Message, etc.).
     */
    public KafkaFireAndForget value(Function<Session, Object> valueFunction) {
        this.valueFunction = valueFunction;
        return this;
    }

    /**
     * Sets the message value using a Scala session function returning any Object.
     */
    public KafkaFireAndForget valueScala(scala.Function1<io.gatling.core.session.Session, Object> valueFunction) {
        this.valueFunction = session -> valueFunction.apply(session.asScala());
        return this;
    }

    // ==================== SERIALIZATION TYPE ====================

    /**
     * Marks the value as a String (default). The value function should
     * return a String.
     */
    public KafkaFireAndForget asString() {
        // Default behavior — no-op, value is used as-is
        return this;
    }

    /**
     * Marks the value as raw bytes. The value function should return byte[].
     */
    public KafkaFireAndForget asBytes() {
        // Value is used as-is — the producer serializer handles byte[]
        return this;
    }

    /**
     * Marks the value as Avro. The value function should return a
     * GenericRecord or SpecificRecord.
     */
    public KafkaFireAndForget asAvro() {
        // Value is used as-is — the Avro serializer handles GenericRecord
        return this;
    }

    /**
     * Marks the value as Protobuf. The value function should return a
     * Protobuf Message.
     */
    public KafkaFireAndForget asProtobuf() {
        // Value is used as-is — the Protobuf serializer handles Message
        return this;
    }

    // ==================== HEADERS ====================

    /**
     * Adds a static header to the message.
     */
    public KafkaFireAndForget header(String key, String value) {
        this.headers.put(key, KafkaDsl.toJavaFunction(value));
        return this;
    }

    /**
     * Adds multiple dynamic headers using a map of string/function pairs.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public KafkaFireAndForget headers(Map<String, Object> newHeaders) {
        for (Map.Entry<String, Object> entry : newHeaders.entrySet()) {
            if (entry.getValue() instanceof Function) {
                this.headers.put(entry.getKey(), (Function<Session, String>) entry.getValue());
            } else if (entry.getValue() instanceof String) {
                this.headers.put(entry.getKey(), KafkaDsl.toJavaFunction((String) entry.getValue()));
            } else {
                throw new IllegalArgumentException("Header value must be a String or Function<Session, String>");
            }
        }
        return this;
    }

    /**
     * Adds a dynamic header using a session function.
     */
    public KafkaFireAndForget header(String key, Function<Session, String> valueFunction) {
        this.headers.put(key, valueFunction);
        return this;
    }

    /**
     * Adds a dynamic header using a Scala session function.
     */
    public KafkaFireAndForget headerScala(String key,
            scala.Function1<io.gatling.core.session.Session, String> valueFunction) {
        this.headers.put(key, session -> valueFunction.apply(session.asScala()));
        return this;
    }

    // ==================== ACK/TIMEOUT ====================

    /**
     * Configures the send to be fire-and-forget (does not wait for broker ack).
     */
    public KafkaFireAndForget noAck() {
        this.waitForAck = false;
        return this;
    }

    /**
     * Configures the send to wait for broker ack (default).
     */
    public KafkaFireAndForget waitForAck() {
        this.waitForAck = true;
        return this;
    }

    /**
     * Sets the timeout for waiting for broker ack.
     */
    public KafkaFireAndForget timeout(long timeout, TimeUnit timeUnit) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        return this;
    }

    // ==================== BUILD ====================

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return KafkaActionBuilder.ofObject(
                requestName, topic, keyFunction, valueFunction,
                headers.isEmpty() ? null : headers,
                waitForAck, timeout, timeUnit).asScala();
    }

    @Override
    public ChainBuilder toChainBuilder() {
        return ActionBuilder.super.toChainBuilder();
    }
}
