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
import pl.perfluencer.kafka.actors.KafkaProducerActor;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import io.gatling.core.CoreComponents;
import scala.collection.immutable.List$;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.pattern.Patterns;
import org.apache.pekko.util.Timeout;
import scala.concurrent.Future;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import java.nio.charset.StandardCharsets;

public class KafkaActionBuilder implements ActionBuilder { // USE io.gatling.core.action.builder.ActionBuilder

    private final String requestName;
    private final String topic;
    private final Function<Session, String> key;
    private final Function<Session, String> value;
    private final java.util.Map<String, Function<Session, String>> headers;
    private final Protocol kafkaProtocol; // Use the interface

    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;

    public KafkaActionBuilder(String requestName, String topic, Function<Session, String> key,
            Function<Session, String> value,
            java.util.Map<String, Function<Session, String>> headers,
            Protocol kafkaProtocol) { // Use interface
        this(requestName, topic, key, value, headers, kafkaProtocol, true, 30, TimeUnit.SECONDS);
    }

    public KafkaActionBuilder(String requestName, String topic, Function<Session, String> key,
            Function<Session, String> value,
            java.util.Map<String, Function<Session, String>> headers,
            Protocol kafkaProtocol, // Use interface
            boolean waitForAck, long timeout, TimeUnit timeUnit) {
        this.requestName = requestName != null ? requestName : "Kafka Request";
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.headers = headers != null ? headers : java.util.Collections.emptyMap();
        this.kafkaProtocol = kafkaProtocol; // Store the Protocol instance
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");

    }

    public KafkaActionBuilder(String requestName, String topic, Function<Session, String> key,
            Function<Session, String> value,
            java.util.Map<String, Function<Session, String>> headers,
            boolean waitForAck, long timeout, TimeUnit timeUnit) {
        this.requestName = requestName != null ? requestName : "Kafka Request";
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.headers = headers != null ? headers : java.util.Collections.emptyMap();
        this.kafkaProtocol = null; // No protocol provided
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");

    }

    public Action build(ScenarioContext ctx, Action next) {
        final Protocol finalKafkaProtocol = (this.kafkaProtocol != null) ? this.kafkaProtocol
                : ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey)
                        .protocol();

        return new KafkaSendAction(requestName, finalKafkaProtocol, ctx.coreComponents(), next, topic, key, value,
                headers,
                waitForAck,
                timeout, timeUnit, ((KafkaProtocolBuilder.KafkaProtocol) finalKafkaProtocol).getProducerRouter());
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() {
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                final Protocol finalKafkaProtocol = (kafkaProtocol != null) ? kafkaProtocol
                        : ctx.protocolComponentsRegistry()
                                .components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();

                return new KafkaSendAction(KafkaActionBuilder.this.requestName, finalKafkaProtocol,
                        ctx.coreComponents(), next,
                        KafkaActionBuilder.this.topic, KafkaActionBuilder.this.key, KafkaActionBuilder.this.value,
                        KafkaActionBuilder.this.headers,
                        KafkaActionBuilder.this.waitForAck, KafkaActionBuilder.this.timeout,
                        KafkaActionBuilder.this.timeUnit,
                        ((KafkaProtocolBuilder.KafkaProtocol) finalKafkaProtocol).getProducerRouter());
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
    private final Action next; // Field to store the next action
    private final CoreComponents coreComponents;
    private final String topic;
    private final Function<Session, String> key;
    private final Function<Session, String> value;
    private final java.util.Map<String, Function<Session, String>> headers;
    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;

    private final ActorRef producerRouter;
    private final String correlationHeaderName;

    public KafkaSendAction(String requestName, Protocol kafkaProtocol, CoreComponents coreComponents, Action next,
            String topic,
            Function<Session, String> key, Function<Session, String> value,
            java.util.Map<String, Function<Session, String>> headers,
            boolean waitForAck, long timeout,
            TimeUnit timeUnit, ActorRef producerRouter) {
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
        this.producerRouter = producerRouter;
        this.correlationHeaderName = ((KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol).getCorrelationHeaderName();
    }

    @Override
    public String name() {
        return "kafka-send-action";
    }

    @Override
    public String toString() {
        return Action.super.toString();
    }

    @Override
    public void $bang(io.gatling.core.session.Session session) {
        Action.super.$bang(session);
    }

    @Override
    public void execute(io.gatling.core.session.Session session) {
        final StatsEngine statsEngine = coreComponents.statsEngine();

        String resolvedKey = key.apply(new Session(session));
        String resolvedValue = value.apply(new Session(session));
        byte[] valueBytes = resolvedValue != null ? resolvedValue.getBytes(StandardCharsets.UTF_8) : null;

        java.util.Map<String, String> resolvedHeaders = new java.util.HashMap<>();
        for (java.util.Map.Entry<String, Function<Session, String>> entry : headers.entrySet()) {
            resolvedHeaders.put(entry.getKey(), entry.getValue().apply(new Session(session)));
        }

        // Use the correlationId as key if available, or generate one?
        // The original code didn't use correlationId for fire-and-forget, but the actor
        // expects one.
        // We can use the key as correlationId or a random UUID.
        // Let's use a random UUID if key is null, or just the key.
        String correlationId = resolvedKey != null ? resolvedKey : java.util.UUID.randomUUID().toString();

        KafkaProducerActor.ProduceMessage message = new KafkaProducerActor.ProduceMessage(
                topic, resolvedKey, valueBytes, correlationId, resolvedHeaders, correlationHeaderName, waitForAck);

        long startTime = coreComponents.clock().nowMillis();

        if (!waitForAck) {
            producerRouter.tell(message, ActorRef.noSender());
            long endTime = coreComponents.clock().nowMillis();
            statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), requestName, startTime,
                    endTime, Status.apply("OK"),
                    scala.Some.apply("200"), scala.Some.apply(""));
            next.execute(session);
            return;
        }

        Timeout askTimeout = new Timeout(timeout, timeUnit);
        Future<Object> future = Patterns.ask(producerRouter, message, askTimeout);

        // Convert Scala Future to Java CompletionStage
        java.util.concurrent.CompletionStage<Object> javaFuture = scala.jdk.javaapi.FutureConverters.asJava(future);

        javaFuture.whenComplete((response, exception) -> {
            long endTime = coreComponents.clock().nowMillis();
            if (exception != null) {
                handleFailure(session, statsEngine, startTime, endTime, exception);
            } else {
                if (response instanceof RecordMetadata) {
                    statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), requestName, startTime,
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
        if (e instanceof KafkaException) {
            statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), name(), startTime, endTime,
                    Status.apply("KO"),
                    scala.Some.apply("500"), scala.Some.apply("KAFKA ERROR: " + e.getMessage()));
            logger.error("Kafka send failed", e);
        } else if (e instanceof TimeoutException || e instanceof java.util.concurrent.TimeoutException) {
            statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), name(), startTime, endTime,
                    Status.apply("KO"),
                    scala.Some.apply("500"), scala.Some.apply("Timeout: " + e.getMessage()));
            logger.error("Error waiting for Kafka send", e);
        } else {
            statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), name(), startTime, endTime,
                    Status.apply("KO"),
                    scala.Some.apply("500"), scala.Some.apply("ERROR: " + e.getMessage()));
            logger.error("Error sending Kafka message", e);
        }
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