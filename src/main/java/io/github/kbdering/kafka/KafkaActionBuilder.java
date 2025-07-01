package io.github.kbdering.kafka;

import com.typesafe.scalalogging.Logger;
import io.gatling.commons.stats.Status;
import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.core.action.Action;
import io.gatling.core.stats.StatsEngine;
import io.gatling.core.structure.ScenarioContext;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import io.gatling.core.CoreComponents;
import scala.collection.immutable.List$;


public class KafkaActionBuilder implements ActionBuilder { //USE io.gatling.core.action.builder.ActionBuilder

    private final String topic;
    private final Function<Session, String> key;
    private final Function<Session, String> value;
    private final Protocol kafkaProtocol; // Use the interface

    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;


    public KafkaActionBuilder(String topic, Function<Session, String> key, Function<Session, String> value, Protocol kafkaProtocol) { //Use interface
        this(topic, key, value, kafkaProtocol, true, 30, TimeUnit.SECONDS);
    }

    public KafkaActionBuilder(String topic, Function<Session, String> key, Function<Session, String> value, Protocol kafkaProtocol, //Use interface
                              boolean waitForAck, long timeout, TimeUnit timeUnit)
    {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.kafkaProtocol = kafkaProtocol; // Store the Protocol instance
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");

    }
    public KafkaActionBuilder(String topic, Function<Session, String> key, Function<Session, String> value,
                              boolean waitForAck, long timeout, TimeUnit timeUnit)
    {
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.kafkaProtocol = null; // No protocol provided
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");

    }



    public Action build(ScenarioContext ctx, Action next) {
        final Protocol finalKafkaProtocol = (this.kafkaProtocol != null) ? this.kafkaProtocol
                : ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();

        return new KafkaSendAction(finalKafkaProtocol, ctx.coreComponents(), next, topic, key, value, waitForAck, timeout, timeUnit);
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() {
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                final Protocol finalKafkaProtocol = (kafkaProtocol != null) ? kafkaProtocol
                        : ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();

                return new KafkaSendAction(finalKafkaProtocol, ctx.coreComponents(), next, KafkaActionBuilder.this.topic, KafkaActionBuilder.this.key, KafkaActionBuilder.this.value, KafkaActionBuilder.this.waitForAck, KafkaActionBuilder.this.timeout, KafkaActionBuilder.this.timeUnit);
            }
        };
    }

    @Override
    public ChainBuilder toChainBuilder() {
        return ActionBuilder.super.toChainBuilder();
    }
}
class KafkaSendAction implements io.gatling.core.action.Action {

    private final io.gatling.core.protocol.Protocol kafkaProtocol;
    private final Action next; // Field to store the next action
    private final CoreComponents coreComponents;
    private final String topic;
    private final Function<Session, String> key;
    private final Function<Session, String> value;
    private final boolean waitForAck;
    private final long timeout;
    private final TimeUnit timeUnit;

    public KafkaSendAction(Protocol kafkaProtocol, CoreComponents coreComponents, Action next, String topic,
                           Function<Session, String> key, Function<Session, String> value, boolean waitForAck, long timeout, TimeUnit timeUnit) {
        this.kafkaProtocol = kafkaProtocol;
        this.next = next;
        this.coreComponents = coreComponents;
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.waitForAck = waitForAck;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
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
    public void execute(io.gatling.core.session.Session session){
        KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol)kafkaProtocol;
        final KafkaProducer<String, String> producer = new KafkaProducer<>(concreteProtocol.getProducerProperties());
        final StatsEngine statsEngine = coreComponents.statsEngine();

        String resolvedKey = key.apply(new Session(session));
        String resolvedValue = value.apply(new Session(session));
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, resolvedKey, resolvedValue);
        CompletableFuture<RecordMetadata> sendFuture = new CompletableFuture<>();
        long startTime = coreComponents.clock().nowMillis();

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                sendFuture.complete(metadata);
            } else {
                sendFuture.completeExceptionally(exception);
            }
        });

        sendFuture.whenComplete((metadata, exception) -> {
            long endTime = coreComponents.clock().nowMillis();

            try {
                if (exception == null) {
                    String transactionName;
                    if (waitForAck) {
                        transactionName = "Kafka Send";
                    } else {
                        transactionName = "Kafka Fire and Forget";
                    }
                    statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), transactionName, startTime, endTime, Status.apply("OK"),
                            scala.Some.apply("500"), scala.Some.apply(""));
                } else {
                    session.markAsFailed();
                    if (exception instanceof KafkaException) {
                        statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), name(), startTime, endTime, Status.apply("KO"),
                                scala.Some.apply("500"), scala.Some.apply("KAFKA ERROR: " + exception.getMessage()));
                        System.err.println("Kafka send failed: " + exception.getMessage());
                    } else {
                        statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), name(), startTime, endTime, Status.apply("KO"),
                                scala.Some.apply("500"), scala.Some.apply("Timeout: " + exception.getMessage()));
                        System.err.println("Error waiting for Kafka send: " + exception.getMessage());
                    }
                }
            } finally {
                try {
                    if (waitForAck && !sendFuture.isDone()) {
                        sendFuture.get(timeout, timeUnit);
                    }
                } catch (TimeoutException e) {
                    session.markAsFailed();
                    statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), name(), startTime, endTime, Status.apply("KO"),
                            scala.Some.apply("500"), scala.Some.apply("Timeout: " + e.getMessage()));
                } catch (Exception e) {
                    session.markAsFailed();
                    statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), name(), startTime, endTime, Status.apply("KO"),
                            scala.Some.apply("500"), scala.Some.apply("ERROR: " + e.getMessage()));
                } finally {
                    next.execute(session);
                    producer.close();
                }
            }
        });
    }

    @Override
    public void com$typesafe$scalalogging$StrictLogging$_setter_$logger_$eq(Logger x$1) {

    }

    @Override
    public Logger logger() {
        return null;
    }
}