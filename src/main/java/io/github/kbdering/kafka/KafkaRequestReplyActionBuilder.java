package io.github.kbdering.kafka;

import akka.actor.*;
import akka.routing.RoundRobinPool;
import com.typesafe.scalalogging.Logger;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.core.action.Action;
import io.gatling.core.structure.ScenarioContext;


import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;

import io.gatling.core.protocol.Protocol;


public class KafkaRequestReplyActionBuilder implements ActionBuilder {

    private final String requestTopic;
    private final String responseTopic;
    private final Function<Session, String> keyFunction;
    private final Function<Session, byte[]> valueFunction; // Value is now byte[]
    private final SerializationType requestSerializationType;
    private final Protocol kafkaProtocol; // Use the interface
    private final List<MessageCheck<?, ?>> messageChecks;
    private final long timeout;
    private final TimeUnit timeUnit;

    // Primary constructor
    public KafkaRequestReplyActionBuilder(String requestTopic, String responseTopic,
                                          Function<Session, String> keyFunction,
                                          Function<Session, byte[]> valueFunction,
                                          SerializationType requestSerializationType,
                                          Protocol kafkaProtocol, 
                                          List<MessageCheck<?, ?>> messageChecks,
                                          long timeout, TimeUnit timeUnit) {
        this.requestTopic = Objects.requireNonNull(requestTopic, "requestTopic must not be null");
        this.responseTopic = responseTopic; //response topic is optional
        this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction must not be null");
        this.valueFunction = Objects.requireNonNull(valueFunction, "valueFunction must not be null");
        this.requestSerializationType = Objects.requireNonNull(requestSerializationType, "requestSerializationType must not be null");
        this.kafkaProtocol = kafkaProtocol;  // Store the Protocol
        this.messageChecks = (messageChecks != null) ? messageChecks : Collections.emptyList();
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");
    }

    public Action build(ScenarioContext ctx, Action next) {
        final Protocol finalKafkaProtocol = (this.kafkaProtocol != null) ? this.kafkaProtocol
                : ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();
        KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol) finalKafkaProtocol;
        ActorSystem system = concreteProtocol.getActorSystem();

        ActorRef producerRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumProducers()).props(KafkaProducerActor.props(concreteProtocol.getProducerProperties())), "kafkaProducerRouter");
        List<MessageCheck<?, ?>> actualChecks = this.messageChecks; // Checks are now directly from this builder
        ActorRef messageProcessorRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumConsumers()).props(MessageProcessorActor.props(concreteProtocol.getRequestStore(), ctx.coreComponents(), actualChecks)), "messageProcessorRouter");
        ActorRef consumerRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumConsumers()).props(KafkaConsumerActor.props(concreteProtocol.getConsumerProperties(), responseTopic, messageProcessorRouter, ctx.coreComponents())), "kafkaConsumerRouter");

        for (int i = 0; i < concreteProtocol.getNumConsumers(); i++) {
            consumerRouter.tell(KafkaMessages.Poll.INSTANCE, ActorRef.noSender()); // Send a specific message to trigger polling
        }

        return new KafkaRequestReplyAction(ctx.coreComponents(), next, finalKafkaProtocol, requestTopic, keyFunction,
                valueFunction, requestSerializationType, timeUnit, timeout, producerRouter);
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() { // Anonymous Scala ActionBuilder
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                io.gatling.core.protocol.Protocol finalKafkaProtocol = (kafkaProtocol != null) ? kafkaProtocol
                        : ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();
                KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol) finalKafkaProtocol;
                ActorSystem system = concreteProtocol.getActorSystem();

                ActorRef producerRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumProducers()).props(KafkaProducerActor.props(concreteProtocol.getProducerProperties())), "kafkaProducerRouter");
                List<MessageCheck<?, ?>> actualChecks = KafkaRequestReplyActionBuilder.this.messageChecks; // Checks are now directly from this builder
                ActorRef messageProcessorRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumConsumers()).props(MessageProcessorActor.props(concreteProtocol.getRequestStore(), ctx.coreComponents(), actualChecks)), "messageProcessorRouter");
                ActorRef consumerRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumConsumers()).props(KafkaConsumerActor.props(concreteProtocol.getConsumerProperties(), responseTopic, messageProcessorRouter, ctx.coreComponents())), "kafkaConsumerRouter");

                for (int i=0; i < concreteProtocol.getNumConsumers(); i++) {
                    consumerRouter.tell(KafkaMessages.Poll.INSTANCE, ActorRef.noSender()); // Send a specific message to trigger polling
                }
                return new KafkaRequestReplyAction(ctx.coreComponents(), next, finalKafkaProtocol, requestTopic, keyFunction,
                        valueFunction, requestSerializationType, timeUnit, timeout, producerRouter);
            }
        };
    }

    @Override
    public ChainBuilder toChainBuilder() {
        return ActionBuilder.super.toChainBuilder();
    }


}

class KafkaRequestReplyAction implements io.gatling.core.action.Action {

    private final io.gatling.core.CoreComponents coreComponents;
    private io.gatling.core.action.Action next;
    private final Protocol kafkaProtocol;
    private final String requestTopic;
    private final Function<Session, String> keyFunction;
    private final Function<Session, byte[]> valueFunction; // Value is now byte[]
    private final SerializationType requestSerializationType;
    private final ActorRef producerRouter;
    // consumerRouter is not needed here as responses are handled by MessageProcessorActor

    private final TimeUnit timeUnit;
    private final long timeout;

    public KafkaRequestReplyAction( io.gatling.core.CoreComponents coreComponents, io.gatling.core.action.Action next, io.gatling.core.protocol.Protocol kafkaProtocol, String requestTopic,
                                    Function<Session, String> keyFunction, Function<Session, byte[]> valueFunction, SerializationType requestSerializationType,
                                    TimeUnit timeUnit, long timeout, ActorRef producerRouter) {
        this.coreComponents = coreComponents;
        this.next = next;
        this.kafkaProtocol = kafkaProtocol;
        this.requestTopic = requestTopic;
        this.keyFunction = keyFunction;
        this.valueFunction = valueFunction;
        this.requestSerializationType = requestSerializationType;
        this.timeUnit = timeUnit;
        this.timeout = timeout;
        this.producerRouter = producerRouter;
    }


    @Override
    public String name() {
        return "kafka-request-reply-action-akka";
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
        KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol;
        Session gatlingJavaSession = new Session(session); // Gatling Java API Session
        String resolvedKey = keyFunction.apply(gatlingJavaSession);
        byte[] resolvedValueBytes = valueFunction.apply(gatlingJavaSession); // Get byte[]
        String correlationId = resolvedKey; //TODO: fix it!!! 
        long startTime = coreComponents.clock().nowMillis();

        // Use the RequestStore to store the request
        try {
            concreteProtocol.getRequestStore().storeRequest(correlationId, resolvedKey, resolvedValueBytes, requestSerializationType, name(),  startTime, timeUnit.toMillis(timeout));
        } catch (Exception e) {
            System.err.println("Error storing request: " + e.getMessage());
            session.markAsFailed();
        }

        KafkaProducerActor.ProduceMessage message = new KafkaProducerActor.ProduceMessage(
                requestTopic, resolvedKey, resolvedValueBytes, correlationId
        );

        producerRouter.tell(message, ActorRef.noSender());
        next.execute(session);
    }

    public void setNext(Action next) {
            this.next = next;
    }

    @Override
    public void com$typesafe$scalalogging$StrictLogging$_setter_$logger_$eq(Logger x$1) {
    }

    @Override
    public Logger logger() {
        return null;
    }
}