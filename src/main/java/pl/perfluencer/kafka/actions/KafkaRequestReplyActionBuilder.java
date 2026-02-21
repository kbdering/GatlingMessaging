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

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.PoisonPill;

import org.apache.kafka.clients.producer.ProducerRecord;

import io.gatling.core.action.Action;
import io.gatling.core.protocol.Protocol;
import io.gatling.core.structure.ScenarioContext;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.Session;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.BiFunction;
import io.gatling.core.CoreComponents;
import io.gatling.core.stats.StatsEngine;

import pl.perfluencer.cache.RequestData;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.common.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;

import io.gatling.commons.stats.Status;
import scala.collection.immutable.List$;

public class KafkaRequestReplyActionBuilder<ReqT, ResT> implements ActionBuilder {

    private final String requestName;
    private final String requestTopic;
    private final String responseTopic;
    private final Function<Session, String> keyFunction;
    private final Function<Session, Object> valueFunction;
    private final java.util.Map<String, Function<Session, String>> headers;

    private final Class<ReqT> requestClass;
    private final SerializationType requestSerde;
    private final Class<ResT> responseClass;
    private final SerializationType responseSerde;

    private final Protocol kafkaProtocol;
    private final List<MessageCheck<?, ?>> messageChecks;
    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;
    private final java.util.List<String> sessionVariablesToStore = new java.util.ArrayList<>();
    private boolean skipPayloadStorage = false;

    /**
     * Stores session variables alongside the request for later correlation.
     * These variables can be accessed in checks or subsequent actions.
     *
     * @param keys The session variable keys to store.
     * @return this builder
     */
    public KafkaRequestReplyActionBuilder<ReqT, ResT> storeSession(String... keys) {
        if (keys != null) {
            java.util.Collections.addAll(this.sessionVariablesToStore, keys);
        }
        return this;
    }

    /**
     * Skips storing the payload in the RequestStore.
     * This is a memory optimization useful for extremely large messages where
     * the payload itself is not required for response verification.
     *
     * @return this builder
     */
    public KafkaRequestReplyActionBuilder<ReqT, ResT> skipPayloadStorage() {
        this.skipPayloadStorage = true;
        return this;
    }

    // ==================== FLUENT CHECK API ====================

    /**
     * Registers a simplified, strongly populated typed check that uses a BiFunction
     * to
     * validate the response against the request.
     *
     * @param checkName The descriptive name of the check.
     * @param logic     The validation logic returning an {@code Optional<String>}
     *                  containing an error message, or empty if successful.
     * @return this builder
     */
    public KafkaRequestReplyActionBuilder<ReqT, ResT> check(String checkName,
            BiFunction<ReqT, ResT, Optional<String>> logic) {
        MessageCheck<ReqT, ResT> check = new MessageCheck<>(
                checkName,
                requestClass, requestSerde,
                responseClass, responseSerde,
                logic);
        this.messageChecks.add(check);
        return this;
    }

    /**
     * Adds a generic {@link MessageCheck} to validate the response.
     *
     * @param check The MessageCheck instance to apply upon receiving a reply.
     * @return this builder
     */
    public KafkaRequestReplyActionBuilder<ReqT, ResT> check(MessageCheck<?, ?> check) {
        this.messageChecks.add(check);
        return this;
    }

    /**
     * Adds multiple {@link MessageCheck}s at once to validate the response.
     *
     * @param checks An array of MessageChecks to add.
     * @return this builder
     */
    public KafkaRequestReplyActionBuilder<ReqT, ResT> checks(MessageCheck<?, ?>... checks) {
        if (checks != null) {
            for (MessageCheck<?, ?> check : checks) {
                this.messageChecks.add(check);
            }
        }
        return this;
    }

    /**
     * Adds a check derived from a {@code MessageCheckBuilder} result.
     * This integrates with the fluent assertions API.
     *
     * @param result The result object from MessageCheckBuilder.
     * @return this builder
     */
    public KafkaRequestReplyActionBuilder<ReqT, ResT> check(
            pl.perfluencer.common.checks.MessageCheckBuilder.MessageCheckResult<?, ?> result) {
        this.messageChecks.add(MessageCheck.from(result, requestSerde, responseSerde));
        return this;
    }

    public KafkaRequestReplyActionBuilder(String requestName, String requestTopic, String responseTopic,
            Function<Session, String> keyFunction, Function<Session, Object> valueFunction,
            Class<ReqT> requestClass, SerializationType requestSerde,
            Class<ResT> responseClass, SerializationType responseSerde,
            Protocol kafkaProtocol,
            List<MessageCheck<?, ?>> messageChecks,
            boolean waitForAck, long timeout, TimeUnit timeUnit) {
        this(requestName, requestTopic, responseTopic, keyFunction, valueFunction, null,
                requestClass, requestSerde, responseClass, responseSerde,
                kafkaProtocol, messageChecks, waitForAck, timeout, timeUnit);
    }

    public KafkaRequestReplyActionBuilder(String requestName, String requestTopic, String responseTopic,
            Function<Session, String> keyFunction, Function<Session, Object> valueFunction,
            java.util.Map<String, Function<Session, String>> headers,
            Class<ReqT> requestClass, SerializationType requestSerde,
            Class<ResT> responseClass, SerializationType responseSerde,
            Protocol kafkaProtocol,
            List<MessageCheck<?, ?>> messageChecks,
            boolean waitForAck, long timeout, TimeUnit timeUnit) {
        this.requestName = requestName;
        this.requestTopic = Objects.requireNonNull(requestTopic, "requestTopic must not be null");
        this.responseTopic = responseTopic;
        this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction must not be null");
        this.valueFunction = Objects.requireNonNull(valueFunction, "valueFunction must not be null");

        this.requestClass = requestClass;
        this.requestSerde = requestSerde;
        this.responseClass = responseClass;
        this.responseSerde = responseSerde;

        this.headers = headers != null ? headers : Collections.emptyMap();
        this.messageChecks = (messageChecks != null) ? new java.util.ArrayList<>(messageChecks)
                : new java.util.ArrayList<>();
        this.kafkaProtocol = kafkaProtocol;
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() {
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                KafkaProtocolBuilder.KafkaProtocolComponents components = ctx.protocolComponentsRegistry()
                        .components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey);
                KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol) components
                        .protocol();

                // Register checks for this request name ALWAYS
                concreteProtocol.registerChecks(requestName, messageChecks);

                if (concreteProtocol.getConsumerAndProcessor(responseTopic) == null) {

                    // Create MessageProcessor (shared across all consumer threads)
                    pl.perfluencer.kafka.MessageProcessor messageProcessor = new pl.perfluencer.kafka.MessageProcessor(
                            concreteProtocol.getRequestStore(),
                            ctx.coreComponents().statsEngine(),
                            ctx.coreComponents().clock(),
                            null, // No actor controller needed for thread-based consumers
                            concreteProtocol.getCheckRegistry(),
                            concreteProtocol.getGlobalChecks(),
                            null, // Session-aware checks not used here
                            concreteProtocol.getCorrelationExtractor(),
                            concreteProtocol.isUseTimestampHeader(),
                            concreteProtocol.getRetryBackoff(),
                            concreteProtocol.getMaxRetries(),
                            concreteProtocol.getParserRegistry());

                    // Create consumer threads
                    LoggerFactory.getLogger(KafkaRequestReplyActionBuilder.class).debug(
                            "Initializing pool of {} consumer threads for topic: {}",
                            concreteProtocol.getNumConsumers(), responseTopic);
                    java.util.List<pl.perfluencer.kafka.consumers.KafkaConsumerThread> consumerThreads = new java.util.ArrayList<>();
                    for (int i = 0; i < concreteProtocol.getNumConsumers(); i++) {
                        pl.perfluencer.kafka.consumers.KafkaConsumerThread thread = new pl.perfluencer.kafka.consumers.KafkaConsumerThread(
                                concreteProtocol.getConsumerProperties(),
                                responseTopic,
                                messageProcessor,
                                ctx.coreComponents().statsEngine(),
                                concreteProtocol.getPollTimeout(),
                                concreteProtocol.isSyncCommit(),
                                i);
                        thread.start();
                        consumerThreads.add(thread);
                    }
                    LoggerFactory.getLogger(KafkaRequestReplyActionBuilder.class).debug(
                            "Successfully started {} consumer threads for topic: {}", consumerThreads.size(),
                            responseTopic);

                    concreteProtocol.putConsumerAndProcessor(responseTopic,
                            new KafkaProtocolBuilder.ConsumerAndProcessor(consumerThreads, messageProcessor));
                }

                return new KafkaRequestReplyAction(requestName, concreteProtocol, ctx.coreComponents(), next,
                        requestTopic, keyFunction,
                        valueFunction, headers, requestSerde, waitForAck, timeout, timeUnit,
                        sessionVariablesToStore, skipPayloadStorage);
            }
        };
    }

    @Override
    public ChainBuilder toChainBuilder() {
        return ActionBuilder.super.toChainBuilder();
    }
}

class KafkaRequestReplyAction implements io.gatling.core.action.Action {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRequestReplyAction.class);
    private static final scala.collection.immutable.List<String> ACK_GROUP = scala.collection.immutable.List$.MODULE$
            .from(
                    scala.jdk.javaapi.CollectionConverters.asScala(java.util.Collections.singletonList("ACK Group")));
    private final String requestName;
    private final Action next;
    private final CoreComponents coreComponents;
    private final String topic;
    private final Function<Session, String> key;
    private final Function<Session, Object> value;
    private final java.util.Map<String, Function<Session, String>> headers;
    private final SerializationType serializationType;
    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;
    private final Protocol kafkaProtocol;
    private final java.util.List<String> sessionVariablesToStore;
    private final boolean skipPayloadStorage;

    public KafkaRequestReplyAction(String requestName, Protocol kafkaProtocol, CoreComponents coreComponents,
            Action next,
            String topic,
            Function<Session, String> key, Function<Session, Object> value,
            java.util.Map<String, Function<Session, String>> headers,
            SerializationType serializationType,
            boolean waitForAck, long timeout,
            TimeUnit timeUnit,
            java.util.List<String> sessionVariablesToStore,
            boolean skipPayloadStorage) {
        this.requestName = requestName;
        this.kafkaProtocol = kafkaProtocol;
        this.next = next;
        this.coreComponents = coreComponents;
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.serializationType = serializationType;
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.sessionVariablesToStore = sessionVariablesToStore;
        this.skipPayloadStorage = skipPayloadStorage;
    }

    @Override
    public String name() {
        return "kafka-request-reply-action";
    }

    @Override
    public void execute(io.gatling.core.session.Session session) {
        final StatsEngine statsEngine = coreComponents.statsEngine();
        final Session javaSession = new Session(session);

        String resolvedKey = key.apply(javaSession);
        Object valueObj = value.apply(javaSession);

        String correlationId;
        pl.perfluencer.kafka.extractors.CorrelationExtractor correlationExtractor = ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol)
                .getCorrelationExtractor();

        if (correlationExtractor instanceof pl.perfluencer.kafka.extractors.KeyExtractor) {
            correlationId = resolvedKey;
            if (correlationId == null) {
                correlationId = java.util.UUID.randomUUID().toString();
            }
        } else {
            correlationId = java.util.UUID.randomUUID().toString();
        }

        long startTime = coreComponents.clock().nowMillis();

        // Store request for correlation
        RequestStore requestStore = ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol).getRequestStore();
        try {
            long storeStart = coreComponents.clock().nowMillis();

            java.util.Map<String, String> sessionVars;
            if (sessionVariablesToStore != null && !sessionVariablesToStore.isEmpty()) {
                sessionVars = new java.util.HashMap<>(sessionVariablesToStore.size());
                for (String k : sessionVariablesToStore) {
                    Object val = javaSession.get(k);
                    if (val != null) {
                        sessionVars.put(k, val.toString());
                    }
                }
            } else {
                sessionVars = java.util.Collections.emptyMap();
            }

            requestStore.storeRequest(new RequestData(correlationId, resolvedKey, skipPayloadStorage ? null : valueObj,
                    serializationType, requestName,
                    session.scenario(), startTime, timeUnit.toMillis(timeout), sessionVars));
            long storeEnd = coreComponents.clock().nowMillis();

            if (((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol).isMeasureStoreLatency()) {
                statsEngine.logResponse(
                        session.scenario(),
                        session.groups(),
                        requestName + " Store",
                        storeStart,
                        storeEnd,
                        Status.apply("OK"),
                        scala.Option.empty(),
                        scala.Option.empty());

                // We do NOT logGroupEnd for "Store" because it runs inside the overall
                // transaction and shares session.groups(),
                // letting Gatling natively handle the parent group. We only manually emit
                // GroupBlocks for our detached Async callbacks.
            }
        } catch (Exception e) {
            logger.error("Failed to store request in RequestStore", e);
            session.markAsFailed();
            ((org.apache.pekko.actor.ActorRef) (Object) coreComponents.controller()).tell(PoisonPill.getInstance(),
                    ActorRef.noSender());
            next.execute(session);
            return;
        }

        java.util.Map<String, String> resolvedHeaders;
        if (headers.isEmpty()) {
            resolvedHeaders = java.util.Collections.emptyMap();
        } else {
            resolvedHeaders = new java.util.HashMap<>(headers.size());
            for (java.util.Map.Entry<String, Function<Session, String>> entry : headers.entrySet()) {
                resolvedHeaders.put(entry.getKey(), entry.getValue().apply(javaSession));
            }
        }

        String correlationHeaderName = ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol).getCorrelationHeaderName();

        org.apache.kafka.clients.producer.KafkaProducer<String, Object> producer = ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol)
                .getProducer();
        if (producer == null) {
            long endTime = coreComponents.clock().nowMillis();
            Exception e = new RuntimeException("No Kafka Producers available. Check configuration.");
            logger.error(e.getMessage());
            handleFailure(session, statsEngine, startTime, endTime, e, correlationId);
            return;
        }

        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, resolvedKey, valueObj);
        if (correlationId != null) {
            record.headers().add(correlationHeaderName, correlationId.getBytes());
        }
        if (resolvedHeaders != null) {
            for (java.util.Map.Entry<String, String> entry : resolvedHeaders.entrySet()) {
                if (entry.getValue() != null) {
                    record.headers().add(entry.getKey(), entry.getValue().getBytes());
                }
            }
        }

        final String finalCorrelationId = correlationId;

        // Wait for ACK
        try {
            producer.send(record, (metadata, exception) -> {
                long endTime = coreComponents.clock().nowMillis();
                if (exception != null) {
                    logger.error("Kafka Producer Send Error (Async Callback)", exception);
                    handleFailure(session, statsEngine, startTime, endTime, exception, finalCorrelationId);
                } else {
                    statsEngine.logResponse(session.scenario(), ACK_GROUP, requestName + " send", startTime,
                            endTime, Status.apply("OK"),
                            scala.Option.empty(), scala.Option.empty());

                    statsEngine.logGroupEnd(session.scenario(),
                            new io.gatling.core.session.GroupBlock(ACK_GROUP, startTime, (int) (endTime - startTime),
                                    Status.apply("OK")),
                            endTime);

                    next.execute(session);
                }
            });
        } catch (Exception e) {
            long endTime = coreComponents.clock().nowMillis();
            logger.error("Kafka Producer Send Error (Immediate)", e);
            handleFailure(session, statsEngine, startTime, endTime, e, finalCorrelationId);
        }
    }

    private void handleFailure(io.gatling.core.session.Session session, StatsEngine statsEngine, long startTime,
            long endTime, Throwable e, String correlationId) {
        RequestStore requestStore = ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol).getRequestStore();
        requestStore.deleteRequest(correlationId);

        session.markAsFailed();
        statsEngine.logResponse(session.scenario(), ACK_GROUP, requestName + " Send Failed", startTime,
                endTime,
                Status.apply("KO"),
                scala.Some.apply("500"), scala.Some.apply("Send Failed: " + e.getMessage()));
        statsEngine.logGroupEnd(session.scenario(),
                new io.gatling.core.session.GroupBlock(ACK_GROUP, startTime, (int) (endTime - startTime),
                        Status.apply("KO")),
                endTime);
        logger.error("Kafka Request-Reply Send failed", e);
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
