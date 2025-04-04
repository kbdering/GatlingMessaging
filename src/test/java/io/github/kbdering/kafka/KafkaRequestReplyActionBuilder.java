package io.github.kbdering.kafka;

import akka.actor.*;
import akka.routing.RoundRobinPool;
import com.typesafe.scalalogging.Logger;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.core.action.Action;
import io.gatling.core.structure.ScenarioContext;


import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;

import io.gatling.core.protocol.Protocol;


public class KafkaRequestReplyActionBuilder implements ActionBuilder {

    private final String requestTopic;
    private final String responseTopic;
    private final Function<Session, String> key;
    private final Function<Session, String> value;
    private final Protocol kafkaProtocol; // Use the interface
    private final long timeout;
    private final TimeUnit timeUnit;

    public KafkaRequestReplyActionBuilder(String requestTopic, String responseTopic, Function<Session, String> key, Function<Session, String> value,
                                          Protocol kafkaProtocol, long timeout, TimeUnit timeUnit) {//Use interface
        this.requestTopic = Objects.requireNonNull(requestTopic, "requestTopic must not be null");
        this.responseTopic = responseTopic; //response topic is optional
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.kafkaProtocol = kafkaProtocol;  // Store the Protocol
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");

    }
    public KafkaRequestReplyActionBuilder(String requestTopic, String responseTopic, Function<Session, String> key, Function<Session, String> value,
                                          long timeout, TimeUnit timeUnit) {
        this.requestTopic = Objects.requireNonNull(requestTopic, "requestTopic must not be null");
        this.responseTopic = responseTopic; //response topic is optional
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.value = Objects.requireNonNull(value, "value must not be null");
        this.kafkaProtocol = null; // No protocol provided
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");


    }

    public Action build(ScenarioContext ctx, Action next) {

        // Use a late-bound KafkaProtocol if it wasn't provided in the constructor.
        final Protocol finalKafkaProtocol = (this.kafkaProtocol != null) ? this.kafkaProtocol
                : ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();
        KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol;
        ActorSystem system = concreteProtocol.getActorSystem();

        ActorRef producerRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumProducers()).props(KafkaProducerActor.props(concreteProtocol.getProducerProperties())), "kafkaProducerRouter");
        ActorRef consumerRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumConsumers()).props(KafkaConsumerActor.props(concreteProtocol.getConsumerProperties(), responseTopic, concreteProtocol.getRequestStore(), ctx.coreComponents())), "kafkaConsumerRouter");

        return new KafkaRequestReplyAction(ctx.coreComponents(), next, finalKafkaProtocol, requestTopic, key,
                value, timeUnit, timeout, producerRouter, consumerRouter);
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() { // Anonymous Scala ActionBuilder
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                io.gatling.core.protocol.Protocol finalKafkaProtocol = (kafkaProtocol != null) ? kafkaProtocol
                        : ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();

                KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol) kafkaProtocol;
                ActorSystem system = concreteProtocol.getActorSystem();

                ActorRef producerRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumProducers()).props(KafkaProducerActor.props(concreteProtocol.getProducerProperties())), "kafkaProducerRouter");
                ActorRef consumerRouter = system.actorOf(new RoundRobinPool(concreteProtocol.getNumConsumers()).props(KafkaConsumerActor.props(concreteProtocol.getConsumerProperties(),responseTopic, concreteProtocol.getRequestStore(), ctx.coreComponents())), "kafkaConsumerRouter");

                //TODO: probably doesn't belong here, is there a better place for a background process ??
                for (int i=0; i < concreteProtocol.getNumConsumers(); i++) {
                    consumerRouter.tell("", ActorRef.noSender());
                }
                return new KafkaRequestReplyAction(ctx.coreComponents(), next, finalKafkaProtocol, requestTopic, key,
                        value, timeUnit, timeout, producerRouter, consumerRouter);

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
    private final Function<Session, String> key;
    private final Function<Session, String> value;
    private ActorRef producerRouter;
    private ActorRef consumerRouter;

    private final TimeUnit timeUnit;
    private final long timeout;

    public KafkaRequestReplyAction( io.gatling.core.CoreComponents coreComponents, io.gatling.core.action.Action next, io.gatling.core.protocol.Protocol kafkaProtocol, String requestTopic,
                                    Function<Session, String> key, Function<Session, String> value,  TimeUnit timeUnit, long timeout, ActorRef producerRouter, ActorRef consumerRouter) {

        this.coreComponents = coreComponents;
        this.next = next;
        this.kafkaProtocol = kafkaProtocol;
        this.requestTopic = requestTopic;
        this.key = key;
        this.value = value;
        this.timeUnit = timeUnit;
        this.timeout = timeout;
        this.consumerRouter = consumerRouter;
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
        String resolvedKey = key.apply(new Session(session));
        String resolvedValue = value.apply(new Session(session));
        String correlationId = UUID.randomUUID().toString();
        long startTime = coreComponents.clock().nowMillis();

        // Use the RequestStore to store the request
        try {
            concreteProtocol.getRequestStore().storeRequest(resolvedKey, resolvedKey, resolvedValue, name(),  startTime, timeUnit.toMillis(timeout));
        } catch (Exception e) {
            System.err.println("Error storing request: " + e.getMessage());
            session.markAsFailed();
        }

        KafkaProducerActor.ProduceMessage message = new KafkaProducerActor.ProduceMessage(
                requestTopic, resolvedKey, resolvedValue, correlationId
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