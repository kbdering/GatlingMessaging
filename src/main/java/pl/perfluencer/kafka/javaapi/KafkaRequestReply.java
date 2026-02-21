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
import io.gatling.javaapi.core.Session;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.actions.KafkaRequestReplyActionBuilder;
import pl.perfluencer.common.util.SerializationType;
import pl.perfluencer.common.checks.MessageCheckBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.BiFunction;

/**
 * Fluent builder for Kafka request-reply actions.
 */
public class KafkaRequestReply<ReqT, ResT> implements ActionBuilder {

    private final String requestName;
    private final String requestTopic;
    private final String responseTopic;
    private final Function<Session, String> keyFunction;
    private final Function<Session, Object> valueFunction;
    private final Class<ReqT> requestClass;
    private final SerializationType requestSerde;
    private final Class<ResT> responseClass;
    private final SerializationType responseSerde;
    private final List<MessageCheck<?, ?>> checks;
    private final Map<String, Function<Session, String>> headers;
    private final long timeout;
    private final TimeUnit timeUnit;
    private final boolean waitForAck;
    private final List<String> sessionVariablesToStore;
    private final boolean skipPayloadStorage;

    @SuppressWarnings("unchecked")
    public KafkaRequestReply(String requestName) {
        this(requestName, null, null, null, null,
                (Class<ReqT>) byte[].class, SerializationType.BYTE_ARRAY,
                (Class<ResT>) byte[].class, SerializationType.BYTE_ARRAY,
                new ArrayList<>(), new HashMap<>(), 10, TimeUnit.SECONDS, true, new ArrayList<>(), false);
    }

    private KafkaRequestReply(String requestName, String requestTopic, String responseTopic,
            Function<Session, String> keyFunction, Function<Session, Object> valueFunction,
            Class<ReqT> requestClass, SerializationType requestSerde,
            Class<ResT> responseClass, SerializationType responseSerde,
            List<MessageCheck<?, ?>> checks, Map<String, Function<Session, String>> headers,
            long timeout, TimeUnit timeUnit, boolean waitForAck,
            List<String> sessionVariablesToStore, boolean skipPayloadStorage) {
        this.requestName = requestName;
        this.requestTopic = requestTopic;
        this.responseTopic = responseTopic;
        this.keyFunction = keyFunction;
        this.valueFunction = valueFunction;
        this.requestClass = requestClass;
        this.requestSerde = requestSerde;
        this.responseClass = responseClass;
        this.responseSerde = responseSerde;
        this.checks = new ArrayList<>(checks);
        this.headers = new HashMap<>(headers);
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.waitForAck = waitForAck;
        this.sessionVariablesToStore = new ArrayList<>(sessionVariablesToStore);
        this.skipPayloadStorage = skipPayloadStorage;
    }

    // ==================== TOPIC CONFIGURATION ====================

    /**
     * Sets the topic to send the request record to.
     *
     * @param topic The Kafka topic name.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> requestTopic(String topic) {
        return new KafkaRequestReply<>(requestName, topic, responseTopic, keyFunction, valueFunction, requestClass,
                requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit, waitForAck,
                sessionVariablesToStore, skipPayloadStorage);
    }

    /**
     * Sets the topic to listen for the reply record on.
     *
     * @param topic The Kafka topic name.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> responseTopic(String topic) {
        return new KafkaRequestReply<>(requestName, requestTopic, topic, keyFunction, valueFunction, requestClass,
                requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit, waitForAck,
                sessionVariablesToStore, skipPayloadStorage);
    }

    /**
     * Sets both the request and response topics simultaneously.
     *
     * @param requestTopic  The topic to send to.
     * @param responseTopic The topic to expect a reply from.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> topics(String requestTopic, String responseTopic) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    // ==================== KEY/VALUE CONFIGURATION ====================

    /**
     * Sets the Kafka record key to a static string.
     *
     * @param key The static string key.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> key(String key) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, session -> key, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    /**
     * Sets the Kafka record key dynamically using the Gatling Session.
     *
     * @param keyFunction A function that extracts or generates the key from the
     *                    Session.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> key(Function<Session, String> keyFunction) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    /**
     * Sets the Kafka request payload to a static string.
     *
     * @param value The static string payload.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> value(String value) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, session -> value,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    /**
     * Sets the Kafka request payload dynamically using the Gatling Session.
     *
     * @param valueFunction A function that generates the payload from the Session.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> value(Function<Session, Object> valueFunction) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    public KafkaRequestReply<ReqT, ResT> valueString(Function<Session, String> valueFunction) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction::apply,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    // ==================== MQ-STYLE ALIASES ====================

    public KafkaRequestReply<ReqT, ResT> send(String payload) {
        return value(payload);
    }

    public KafkaRequestReply<ReqT, ResT> send(Function<Session, Object> payloadFunction) {
        return value(payloadFunction);
    }

    public KafkaRequestReply<ReqT, ResT> to(String topic) {
        return requestTopic(topic);
    }

    public KafkaRequestReply<ReqT, ResT> replyFrom(String topic) {
        return responseTopic(topic);
    }

    // ==================== LITERATE DSL ALIASES ====================

    public KafkaRequestReply<ReqT, ResT> payload(String payload) {
        return value(payload);
    }

    public KafkaRequestReply<ReqT, ResT> payload(Function<Session, Object> payloadFunction) {
        return value(payloadFunction);
    }

    public KafkaRequestReply<ReqT, ResT> correlateBy(String key) {
        return key(key);
    }

    public KafkaRequestReply<ReqT, ResT> correlateBy(Function<Session, String> keyFunction) {
        return key(keyFunction);
    }

    public KafkaRequestReply<ReqT, ResT> validate(MessageCheck<?, ?> check) {
        return check(check);
    }

    public KafkaRequestReply<ReqT, ResT> validate(MessageCheckBuilder.MessageCheckResult<?, ?> result) {
        return check(result);
    }

    // ==================== SERIALIZATION ====================

    public <NewReq, NewRes> KafkaRequestReply<NewReq, NewRes> serializationType(Class<NewReq> reqClass,
            Class<NewRes> resClass, SerializationType type) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction, reqClass,
                type, resClass, type, checks, headers, timeout, timeUnit, waitForAck, sessionVariablesToStore,
                skipPayloadStorage);
    }

    public KafkaRequestReply<String, String> asString() {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                String.class, SerializationType.STRING, String.class, SerializationType.STRING, checks, headers,
                timeout, timeUnit, waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    public <NewReq, NewRes> KafkaRequestReply<NewReq, NewRes> asProtobuf(Class<NewReq> reqClass,
            Class<NewRes> resClass) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction, reqClass,
                SerializationType.PROTOBUF, resClass, SerializationType.PROTOBUF, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    public <NewReq, NewRes> KafkaRequestReply<NewReq, NewRes> asAvro(Class<NewReq> reqClass, Class<NewRes> resClass) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction, reqClass,
                SerializationType.AVRO, resClass, SerializationType.AVRO, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    public KafkaRequestReply<byte[], byte[]> asByteArray() {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                byte[].class, SerializationType.BYTE_ARRAY, byte[].class, SerializationType.BYTE_ARRAY, checks, headers,
                timeout, timeUnit, waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    // ==================== HEADERS ====================

    public KafkaRequestReply<ReqT, ResT> header(String key, String value) {
        headers.put(key, session -> value);
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    public KafkaRequestReply<ReqT, ResT> header(String key, Function<Session, String> valueFunction) {
        headers.put(key, valueFunction);
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, this.valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    // ==================== FLUENT CHECK API ====================

    /**
     * Registers a simplified, strongly typed check that uses a BiFunction to
     * validate the response against the request.
     *
     * @param checkName The descriptive name of the check.
     * @param logic     The validation logic returning an {@code Optional<String>}
     *                  containing an error message, or empty if successful.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> check(String checkName, BiFunction<ReqT, ResT, Optional<String>> logic) {
        checks.add(new MessageCheck<>(checkName, requestClass, requestSerde, responseClass, responseSerde, logic));
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    /**
     * Adds a generic {@link MessageCheck} to validate the response.
     *
     * @param check The MessageCheck instance to apply upon receiving a reply.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> check(MessageCheck<?, ?> check) {
        checks.add(check);
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    public KafkaRequestReply<ReqT, ResT> checks(MessageCheck<?, ?>... checksArr) {
        if (checksArr != null) {
            for (MessageCheck<?, ?> check : checksArr) {
                this.checks.add(check);
            }
        }
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    /**
     * Adds multiple generic {@link MessageCheck}s from an Iterable.
     *
     * @param checksList An iterable of MessageCheck instances.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> checks(Iterable<? extends MessageCheck<?, ?>> checksList) {
        if (checksList != null) {
            for (MessageCheck<?, ?> check : checksList) {
                this.checks.add(check);
            }
        }
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    public KafkaRequestReply<ReqT, ResT> check(MessageCheckBuilder.MessageCheckResult<?, ?> result) {
        checks.add(MessageCheck.from(result, requestSerde, responseSerde));
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    // ==================== TIMEOUT CONFIGURATION ====================

    /**
     * Sets the maximum time to wait for the reply record.
     *
     * @param value The timeout magnitude.
     * @param unit  The timeout unit.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> timeout(long value, TimeUnit unit) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, value, unit, waitForAck,
                sessionVariablesToStore, skipPayloadStorage);
    }

    public KafkaRequestReply<ReqT, ResT> timeout(Duration duration) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, duration.toMillis(),
                TimeUnit.MILLISECONDS, waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    // ==================== ACK CONFIGURATION ====================

    /**
     * Sets whether to wait for broker acknowledgement before proceeding in the
     * scenario.
     * 
     * @param wait true to wait for ACK, false otherwise.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> waitForAck(boolean wait) {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit, wait,
                sessionVariablesToStore, skipPayloadStorage);
    }

    public KafkaRequestReply<ReqT, ResT> noAck() {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit, false,
                sessionVariablesToStore, skipPayloadStorage);
    }

    // ==================== SESSION STORAGE ====================

    /**
     * Stores session variables alongside the request for later correlation.
     * These variables can be accessed in checks or subsequent actions.
     *
     * @param keys The session variable keys to store.
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> storeSession(String... keys) {
        if (keys != null) {
            java.util.Collections.addAll(this.sessionVariablesToStore, keys);
        }
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, skipPayloadStorage);
    }

    /**
     * Skips storing the payload in the RequestStore.
     * This is a memory optimization useful for extremely large messages where
     * the payload itself is not required for response verification.
     *
     * @return a new builder instance
     */
    public KafkaRequestReply<ReqT, ResT> skipPayloadStorage() {
        return new KafkaRequestReply<>(requestName, requestTopic, responseTopic, keyFunction, valueFunction,
                requestClass, requestSerde, responseClass, responseSerde, checks, headers, timeout, timeUnit,
                waitForAck, sessionVariablesToStore, true);
    }

    // ==================== BUILD ====================

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        KafkaRequestReplyActionBuilder<ReqT, ResT> builder = new KafkaRequestReplyActionBuilder<>(
                requestName,
                requestTopic,
                responseTopic,
                keyFunction,
                valueFunction,
                headers.isEmpty() ? null : headers,
                requestClass, requestSerde, responseClass, responseSerde,
                null, // protocol resolved from context
                checks,
                waitForAck,
                timeout,
                timeUnit);

        if (!sessionVariablesToStore.isEmpty()) {
            builder.storeSession(sessionVariablesToStore.toArray(new String[0]));
        }
        if (skipPayloadStorage) {
            builder.skipPayloadStorage();
        }

        return builder.asScala();
    }
}
