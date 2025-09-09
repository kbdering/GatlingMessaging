package io.github.kbdering.kafka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.routing.RoundRobinPool;
import com.typesafe.scalalogging.Logger;
import io.gatling.core.action.Action;
import io.gatling.core.protocol.Protocol;
import io.gatling.core.structure.ScenarioContext;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.Session;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class KafkaRequestReplyActionBuilder implements ActionBuilder {

    private final String requestTopic;
    private final String responseTopic;
    private final Function<Session, String> keyFunction;
    private final Function<Session, byte[]> valueFunction;
    private final SerializationType requestSerializationType;
    private final Protocol kafkaProtocol;
    private final List<MessageCheck<?, ?>> messageChecks;
    private final long timeout;
    private final TimeUnit timeUnit;

    public KafkaRequestReplyActionBuilder(String requestTopic, String responseTopic,
                                          Function<Session, String> keyFunction,
                                          Function<Session, byte[]> valueFunction,
                                          SerializationType requestSerializationType,
                                          Protocol kafkaProtocol,
                                          List<MessageCheck<?, ?>> messageChecks,
                                          long timeout, TimeUnit timeUnit) {
        this.requestTopic = Objects.requireNonNull(requestTopic, "requestTopic must not be null");
        this.responseTopic = responseTopic;
        this.keyFunction = Objects.requireNonNull(keyFunction, "keyFunction must not be null");
        this.valueFunction = Objects.requireNonNull(valueFunction, "valueFunction must not be null");
        this.requestSerializationType = Objects.requireNonNull(requestSerializationType, "requestSerializationType must not be null");
        this.kafkaProtocol = kafkaProtocol;
        this.messageChecks = (messageChecks != null) ? messageChecks : Collections.emptyList();
        this.timeout = timeout;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit must not be null");
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() {
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                KafkaProtocolBuilder.KafkaProtocolComponents components = ctx.protocolComponentsRegistry().components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey);
                KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol) components.protocol();

                if (concreteProtocol.getConsumerAndProcessor(responseTopic) == null) {
                    ActorSystem system = concreteProtocol.getActorSystem();
                    ActorRef messageProcessorRouter = system.actorOf(
                            new RoundRobinPool(concreteProtocol.getNumConsumers()).props(MessageProcessorActor.props(concreteProtocol.getRequestStore(), ctx.coreComponents(), messageChecks)),
                            "messageProcessorRouter-" + responseTopic
                    );
                    ActorRef consumerRouter = system.actorOf(
                            new RoundRobinPool(concreteProtocol.getNumConsumers()).props(KafkaConsumerActor.props(concreteProtocol.getConsumerProperties(), responseTopic, messageProcessorRouter, ctx.coreComponents())),
                            "kafkaConsumerRouter-" + responseTopic
                    );

                    for (int i = 0; i < concreteProtocol.getNumConsumers(); i++) {
                        consumerRouter.tell(KafkaMessages.Poll.INSTANCE, ActorRef.noSender());
                    }
                    concreteProtocol.putConsumerAndProcessor(responseTopic, new KafkaProtocolBuilder.ConsumerAndProcessor(consumerRouter, messageProcessorRouter));
                }

                return new KafkaRequestReplyAction(ctx.coreComponents(), next, concreteProtocol, requestTopic, keyFunction,
                        valueFunction, requestSerializationType, timeUnit, timeout, concreteProtocol.getProducerRouter());
            }
        };
    }

    @Override
    public ChainBuilder toChainBuilder() {
        return ActionBuilder.super.toChainBuilder();
    }
}

class KafkaRequestReplyAction implements Action {

    private final io.gatling.core.CoreComponents coreComponents;
    private Action next;
    private final KafkaProtocolBuilder.KafkaProtocol kafkaProtocol;
    private final String requestTopic;
    private final Function<Session, String> keyFunction;
    private final Function<Session, byte[]> valueFunction;
    private final SerializationType requestSerializationType;
    private final ActorRef producerRouter;
    private final TimeUnit timeUnit;
    private final long timeout;

    public KafkaRequestReplyAction(io.gatling.core.CoreComponents coreComponents, Action next, KafkaProtocolBuilder.KafkaProtocol kafkaProtocol, String requestTopic,
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
    public void execute(io.gatling.core.session.Session session) {
        Session gatlingJavaSession = new Session(session);
        String resolvedKey = keyFunction.apply(gatlingJavaSession);
        byte[] resolvedValueBytes = valueFunction.apply(gatlingJavaSession);
        String correlationId = resolvedKey;
        long startTime = coreComponents.clock().nowMillis();

        try {
            kafkaProtocol.getRequestStore().storeRequest(correlationId, resolvedKey, resolvedValueBytes, requestSerializationType, name(), startTime, timeUnit.toMillis(timeout));
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

    @Override
    public void com$typesafe$scalalogging$StrictLogging$_setter_$logger_$eq(Logger x$1) {
    }

    @Override
    public Logger logger() {
        return null;
    }
}


