package io.github.kbdering.kafka.actions;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.routing.RoundRobinPool;
import io.gatling.core.action.Action;
import io.gatling.core.protocol.Protocol;
import io.gatling.core.structure.ScenarioContext;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.Session;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import io.gatling.core.CoreComponents;
import io.gatling.core.stats.StatsEngine;
import java.nio.charset.StandardCharsets;
import io.github.kbdering.kafka.cache.RequestStore;
import io.github.kbdering.kafka.util.SerializationType;
import io.github.kbdering.kafka.MessageCheck;
import io.github.kbdering.kafka.actors.MessageProcessorActor;
import io.github.kbdering.kafka.actors.KafkaConsumerActor;
import io.github.kbdering.kafka.actors.KafkaMessages;
import io.github.kbdering.kafka.actors.KafkaProducerActor;

import org.apache.pekko.pattern.Patterns;
import org.apache.pekko.util.Timeout;
import scala.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;
import io.gatling.commons.stats.Status;
import scala.collection.immutable.List$;

public class KafkaRequestReplyActionBuilder implements ActionBuilder {

    private final String requestName;
    private final String requestTopic;
    private final String responseTopic;
    private final Function<Session, String> keyFunction;
    private final Function<Session, Object> valueFunction;
    private final SerializationType requestSerializationType;
    private final Protocol kafkaProtocol;
    private final List<MessageCheck<?, ?>> messageChecks;
    private final java.util.Map<String, Function<Session, String>> headers;
    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;

    public KafkaRequestReplyActionBuilder(String requestName, String requestTopic, String responseTopic,
            Function<Session, String> keyFunction,
            Function<Session, Object> valueFunction,
            SerializationType requestSerializationType,
            Protocol kafkaProtocol,
            List<MessageCheck<?, ?>> messageChecks,
            boolean waitForAck,
            long timeout, TimeUnit timeUnit) {
        this(requestName, requestTopic, responseTopic, keyFunction, valueFunction, null, requestSerializationType,
                kafkaProtocol, messageChecks, waitForAck, timeout, timeUnit);
    }

    public KafkaRequestReplyActionBuilder(String requestName, String requestTopic, String responseTopic,
            Function<Session, String> keyFunction,
            Function<Session, Object> valueFunction,
            java.util.Map<String, Function<Session, String>> headers,
            SerializationType requestSerializationType,
            Protocol kafkaProtocol,
            List<MessageCheck<?, ?>> messageChecks,
            boolean waitForAck,
            long timeout, TimeUnit timeUnit) {
        this.requestName = requestName;
        this.requestTopic = Objects.requireNonNull(requestTopic, "requestTopic must not be null");
        this.responseTopic = responseTopic;
        this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction must not be null");
        this.valueFunction = Objects.requireNonNull(valueFunction, "valueFunction must not be null");
        this.requestSerializationType = Objects.requireNonNull(requestSerializationType,
                "requestSerializationType must not be null");
        this.headers = headers != null ? headers : Collections.emptyMap();
        this.kafkaProtocol = kafkaProtocol;
        this.messageChecks = (messageChecks != null) ? messageChecks : Collections.emptyList();
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

                if (concreteProtocol.getConsumerAndProcessor(responseTopic) == null) {
                    ActorSystem system = concreteProtocol.getActorSystem();
                    ActorRef messageProcessorRouter = system.actorOf(
                            new RoundRobinPool(concreteProtocol.getNumConsumers()).props(MessageProcessorActor
                                    .props(concreteProtocol.getRequestStore(), ctx.coreComponents(), messageChecks,
                                            concreteProtocol.getCorrelationHeaderName())),
                            "messageProcessorRouter-" + responseTopic);
                    ActorRef consumerRouter = system.actorOf(
                            new RoundRobinPool(concreteProtocol.getNumConsumers())
                                    .props(KafkaConsumerActor.props(concreteProtocol.getConsumerProperties(),
                                            responseTopic, messageProcessorRouter, ctx.coreComponents(),
                                            concreteProtocol.getPollTimeout())),
                            "kafkaConsumerRouter-" + responseTopic);

                    for (int i = 0; i < concreteProtocol.getNumConsumers(); i++) {
                        consumerRouter.tell(KafkaMessages.Poll.INSTANCE, ActorRef.noSender());
                    }
                    concreteProtocol.putConsumerAndProcessor(responseTopic,
                            new KafkaProtocolBuilder.ConsumerAndProcessor(consumerRouter, messageProcessorRouter));
                }

                return new KafkaRequestReplyAction(requestName, concreteProtocol, ctx.coreComponents(), next,
                        requestTopic, keyFunction,
                        valueFunction, headers, requestSerializationType, waitForAck, timeout, timeUnit);
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

    public KafkaRequestReplyAction(String requestName, Protocol kafkaProtocol, CoreComponents coreComponents,
            Action next,
            String topic,
            Function<Session, String> key, Function<Session, Object> value,
            java.util.Map<String, Function<Session, String>> headers,
            SerializationType serializationType,
            boolean waitForAck, long timeout,
            TimeUnit timeUnit) {
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
    }

    @Override
    public String name() {
        return "kafka-request-reply-action";
    }

    @Override
    public void execute(io.gatling.core.session.Session session) {
        final StatsEngine statsEngine = coreComponents.statsEngine();
        String resolvedKey = key.apply(new Session(session));
        Object valueObj = value.apply(new Session(session));

        String correlationId = java.util.UUID.randomUUID().toString();
        long startTime = coreComponents.clock().nowMillis();

        // Store request for correlation
        RequestStore requestStore = ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol).getRequestStore();
        requestStore.storeRequest(correlationId, resolvedKey, valueObj, serializationType, requestName,
                session.scenario(), startTime, timeUnit.toMillis(timeout));

        java.util.Map<String, String> resolvedHeaders = new java.util.HashMap<>();
        for (java.util.Map.Entry<String, Function<Session, String>> entry : headers.entrySet()) {
            resolvedHeaders.put(entry.getKey(), entry.getValue().apply(new Session(session)));
        }

        KafkaProducerActor.ProduceMessage message = new KafkaProducerActor.ProduceMessage(
                topic, resolvedKey, valueObj, correlationId, resolvedHeaders,
                ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol).getCorrelationHeaderName(), waitForAck);

        ActorRef producerRouter = ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol).getProducerRouter();

        if (!waitForAck) {
            producerRouter.tell(message, ActorRef.noSender());
            // In fire-and-forget for request-reply, we don't log a separate "Send" metric
            // because the user cares about the E2E transaction.
            // Or should we? The user asked to "measure the duration it takes to send".
            // If waitForAck is false, the duration is effectively 0 (async).
            next.execute(session);
            return;
        }

        // Wait for ACK
        Timeout askTimeout = new Timeout(timeout, timeUnit);
        Future<Object> future = Patterns.ask(producerRouter, message, askTimeout);
        java.util.concurrent.CompletionStage<Object> javaFuture = scala.jdk.javaapi.FutureConverters.asJava(future);

        javaFuture.whenComplete((response, exception) -> {
            long endTime = coreComponents.clock().nowMillis();
            if (exception != null) {
                // If send fails, the whole transaction fails
                handleFailure(session, statsEngine, startTime, endTime, exception);
            } else {
                if (response instanceof RecordMetadata) {
                    // Log the SEND duration
                    statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), requestName + "-Send", startTime,
                            endTime, Status.apply("OK"),
                            scala.Some.apply("200"), scala.Some.apply(""));
                    next.execute(session);
                } else if (response instanceof org.apache.pekko.actor.Status.Failure) {
                    Throwable e = ((org.apache.pekko.actor.Status.Failure) response).cause();
                    handleFailure(session, statsEngine, startTime, endTime, e);
                } else {
                    handleFailure(session, statsEngine, startTime, endTime,
                            new RuntimeException("Unknown response from actor: " + response));
                }
            }
        });
    }

    private void handleFailure(io.gatling.core.session.Session session, StatsEngine statsEngine, long startTime,
            long endTime, Throwable e) {
        session.markAsFailed();
        statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), requestName + "-Send", startTime, endTime,
                Status.apply("KO"),
                scala.Some.apply("500"), scala.Some.apply("Send Failed: " + e.getMessage()));
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
