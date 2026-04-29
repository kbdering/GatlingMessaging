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

package pl.perfluencer.kafka.actions;

import io.gatling.commons.stats.Status;
import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.core.action.Action;
import io.gatling.core.stats.StatsEngine;
import io.gatling.core.structure.ScenarioContext;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.concurrent.TimeUnit;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.gatling.core.CoreComponents;
import scala.collection.immutable.List$;

/**
 * ActionBuilder for Kafka fire-and-forget message sending.
 * <p>
 * Supports String, byte[], Avro GenericRecord, and Protobuf value types.
 * Use the static factory methods ({@code ofString}, {@code ofObject},
 * {@code ofBytes}) or the legacy constructors for backward compatibility.
 * </p>
 */
public class KafkaActionBuilder implements ActionBuilder {

    private final String requestName;
    private final String topic;
    private final Function<Session, String> key;
    private final Function<Session, Object> value;
    private final java.util.Map<String, Function<Session, String>> headers;
    private final Protocol kafkaProtocol;

    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;

    // ========================================================================
    // Single private canonical constructor — all factories delegate here
    // ========================================================================

    private KafkaActionBuilder(String requestName, String topic,
            Function<Session, String> key, Function<Session, Object> value,
            java.util.Map<String, Function<Session, String>> headers,
            Protocol kafkaProtocol, boolean waitForAck, long timeout, TimeUnit timeUnit) {
        this.requestName = requestName != null ? requestName : "Kafka Request";
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.headers = headers != null ? headers : java.util.Collections.emptyMap();
        this.kafkaProtocol = kafkaProtocol;
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");
    }

    // ========================================================================
    // Static factory methods — preferred API
    // ========================================================================

    /**
     * Create a builder that sends a String value (converted to UTF-8 bytes).
     */
    public static KafkaActionBuilder ofString(String requestName, String topic,
            Function<Session, String> key, Function<Session, String> value,
            java.util.Map<String, Function<Session, String>> headers,
            Protocol protocol, boolean waitForAck, long timeout, TimeUnit timeUnit) {
        return new KafkaActionBuilder(requestName, topic, key, wrapStringValue(value),
                headers, protocol, waitForAck, timeout, timeUnit);
    }

    /**
     * Create a builder that sends a String value (simple form).
     */
    public static KafkaActionBuilder ofString(String requestName, String topic,
            Function<Session, String> key, Function<Session, String> value,
            java.util.Map<String, Function<Session, String>> headers,
            Protocol protocol) {
        return ofString(requestName, topic, key, value, headers, protocol, true, 30, TimeUnit.SECONDS);
    }

    /**
     * Create a builder that sends an Object value directly.
     * The Kafka producer serializer handles the actual type.
     */
    public static KafkaActionBuilder ofObject(String requestName, String topic,
            Function<Session, String> key, Function<Session, Object> value,
            java.util.Map<String, Function<Session, String>> headers,
            boolean waitForAck, long timeout, TimeUnit timeUnit) {
        return new KafkaActionBuilder(requestName, topic, key, value, headers, null,
                waitForAck, timeout, timeUnit);
    }

    /**
     * Create a builder that sends an Object value directly (simple form).
     */
    public static KafkaActionBuilder ofObject(String requestName, String topic,
            Function<Session, String> key, Function<Session, Object> value) {
        return ofObject(requestName, topic, key, value, null, true, 30, TimeUnit.SECONDS);
    }

    /**
     * Create a builder that sends byte[] values.
     */
    public static KafkaActionBuilder ofBytes(String requestName, String topic,
            Function<Session, String> key, Function<Session, byte[]> value) {
        return new KafkaActionBuilder(requestName, topic, key,
                session -> value.apply(session),
                null, null, true, 30, TimeUnit.SECONDS);
    }

    // ========================================================================
    // Legacy constructors — backward compatibility with existing DSL callers
    // ========================================================================

    /**
     * @deprecated Use {@link #ofString} factory method instead.
     */
    public KafkaActionBuilder(String requestName, String topic, Function<Session, String> key,
            Function<Session, String> value,
            java.util.Map<String, Function<Session, String>> headers,
            Protocol kafkaProtocol) {
        this(requestName, topic, key, wrapStringValue(value), headers, kafkaProtocol,
                true, 30, TimeUnit.SECONDS);
    }

    /**
     * @deprecated Use {@link #ofString} factory method instead.
     */
    public KafkaActionBuilder(String requestName, String topic, Function<Session, String> key,
            Function<Session, String> value,
            java.util.Map<String, Function<Session, String>> headers,
            boolean waitForAck, long timeout, TimeUnit timeUnit) {
        this(requestName, topic, key, wrapStringValue(value), headers, null,
                waitForAck, timeout, timeUnit);
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /**
     * Wraps a String-returning function as an Object-returning function
     * that converts strings to UTF-8 bytes.
     */
    private static Function<Session, Object> wrapStringValue(Function<Session, String> stringFn) {
        return session -> {
            String s = stringFn.apply(session);
            return s != null ? s.getBytes(StandardCharsets.UTF_8) : null;
        };
    }

    // Kept for backward compatibility with existing DSL code
    /** @deprecated Use {@link #ofObject} factory method instead. */
    public static KafkaActionBuilder withObjectValue(String requestName, String topic,
            Function<Session, String> key, Function<Session, Object> value,
            java.util.Map<String, Function<Session, String>> headers,
            boolean waitForAck, long timeout, TimeUnit timeUnit) {
        return ofObject(requestName, topic, key, value, headers, waitForAck, timeout, timeUnit);
    }

    /** @deprecated Use {@link #ofObject} factory method instead. */
    public static KafkaActionBuilder withObjectValue(String requestName, String topic,
            Function<Session, String> key, Function<Session, Object> value) {
        return ofObject(requestName, topic, key, value);
    }

    // ========================================================================
    // Build
    // ========================================================================

    public Action build(ScenarioContext ctx, Action next) {
        final Protocol finalKafkaProtocol = (this.kafkaProtocol != null) ? this.kafkaProtocol
                : ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey)
                        .protocol();

        return new KafkaSendAction(requestName, (KafkaProtocolBuilder.KafkaProtocol) finalKafkaProtocol,
                ctx.coreComponents(), next, topic, key, value,
                headers, waitForAck, timeout, timeUnit);
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() {
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                final Protocol finalKafkaProtocol = (kafkaProtocol != null) ? kafkaProtocol
                        : ctx.protocolComponentsRegistry()
                                .components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();

                return new KafkaSendAction(KafkaActionBuilder.this.requestName,
                        (KafkaProtocolBuilder.KafkaProtocol) finalKafkaProtocol,
                        ctx.coreComponents(), next,
                        KafkaActionBuilder.this.topic, KafkaActionBuilder.this.key, KafkaActionBuilder.this.value,
                        KafkaActionBuilder.this.headers,
                        KafkaActionBuilder.this.waitForAck, KafkaActionBuilder.this.timeout,
                        KafkaActionBuilder.this.timeUnit);
            }
        };
    }

    @Override
    public ChainBuilder toChainBuilder() {
        return ActionBuilder.super.toChainBuilder();
    }
}

class KafkaSendAction implements io.gatling.core.action.Action {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSendAction.class);
    private final String requestName;
    private final Action next;
    private final CoreComponents coreComponents;
    private final String topic;
    private final Function<Session, String> key;
    private final Function<Session, Object> value;
    private final java.util.Map<String, Function<Session, String>> headers;
    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;

    private final KafkaProtocolBuilder.KafkaProtocol kafkaProtocol;
    private final String correlationHeaderName;

    public KafkaSendAction(String requestName, KafkaProtocolBuilder.KafkaProtocol kafkaProtocol,
            CoreComponents coreComponents, Action next,
            String topic,
            Function<Session, String> key, Function<Session, Object> value,
            java.util.Map<String, Function<Session, String>> headers,
            boolean waitForAck, long timeout,
            TimeUnit timeUnit) {
        this.requestName = requestName;
        this.next = next;
        this.coreComponents = coreComponents;
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.kafkaProtocol = kafkaProtocol;
        this.correlationHeaderName = kafkaProtocol.getCorrelationHeaderName();
    }

    @Override
    public String name() {
        return "kafka-send-action";
    }

    @Override
    public void execute(io.gatling.core.session.Session session) {
        final StatsEngine statsEngine = coreComponents.statsEngine();
        final Session javaSession = new Session(session);

        long startTime = coreComponents.clock().nowMillis();

        try {
            String resolvedKey = key.apply(javaSession);
            Object resolvedValue = value.apply(javaSession);

            java.util.Map<String, String> resolvedHeaders;
            if (headers.isEmpty()) {
                resolvedHeaders = java.util.Collections.emptyMap();
            } else {
                resolvedHeaders = new java.util.HashMap<>(headers.size());
                for (java.util.Map.Entry<String, Function<Session, String>> entry : headers.entrySet()) {
                    resolvedHeaders.put(entry.getKey(), entry.getValue().apply(javaSession));
                }
            }

            String correlationId = resolvedKey != null ? resolvedKey : java.util.UUID.randomUUID().toString();

            org.apache.kafka.clients.producer.KafkaProducer<String, Object> producer = kafkaProtocol.getProducer();
            if (producer == null) {
                handleFailure(session, statsEngine, startTime, startTime,
                        new RuntimeException("No Kafka Producers available"));
                return;
            }

            // The value is sent directly — the Kafka producer serializer handles the type
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, resolvedKey, resolvedValue);
            if (correlationId != null) {
                record.headers().add(correlationHeaderName, correlationId.getBytes(StandardCharsets.UTF_8));
            }
            if (resolvedHeaders != null) {
                for (java.util.Map.Entry<String, String> entry : resolvedHeaders.entrySet()) {
                    if (entry.getValue() != null) {
                        record.headers().add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
                    }
                }
            }

            if (!waitForAck) {
                // Fire and forget
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Kafka Async Send Failed", exception);
                    }
                });
                long endTime = coreComponents.clock().nowMillis();
                statsEngine.logResponse(session.scenario(), session.groups(), requestName + " send", startTime,
                        endTime, Status.apply("OK"),
                        scala.Some.apply("200"), scala.Some.apply("Async Send"));

                next.execute(session);
                return;
            }

            // Wait for Ack
            producer.send(record, (metadata, exception) -> {
                long endTime = coreComponents.clock().nowMillis();
                if (exception != null) {
                    handleFailure(session, statsEngine, startTime, endTime, exception);
                } else {
                    statsEngine.logResponse(session.scenario(), session.groups(), requestName + " send", startTime,
                            endTime, Status.apply("OK"),
                            scala.Some.apply("200"), scala.Option.empty());

                    next.execute(session);
                }
            });
        } catch (Exception e) {
            handleFailure(session, statsEngine, startTime, coreComponents.clock().nowMillis(), e);
        }
    }

    private void handleFailure(io.gatling.core.session.Session session, StatsEngine statsEngine, long startTime,
            long endTime, Throwable e) {
        session.markAsFailed();

        String errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
        if (e.getCause() != null) {
            errorMessage += " (Cause: " + e.getCause().getMessage() + ")";
        }

        statsEngine.logResponse(session.scenario(), session.groups(), name(), startTime, endTime,
                Status.apply("KO"),
                scala.Some.apply("500"), scala.Some.apply(errorMessage));

        logger.error("Error sending Kafka message", e);
        next.execute(session);
    }

    @Override
    public void com$typesafe$scalalogging$StrictLogging$_setter_$logger_$eq(com.typesafe.scalalogging.Logger x$1) {
    }

    @Override
    public com.typesafe.scalalogging.Logger logger() {
        return null;
    }
}