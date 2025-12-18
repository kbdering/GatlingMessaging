package pl.perfluencer.kafka.actors;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.ActorRef;
import io.gatling.core.CoreComponents;
import io.gatling.core.stats.StatsEngine;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import pl.perfluencer.kafka.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public class MessageProcessorActor extends AbstractActor {

    private final MessageProcessor messageProcessor;

    public MessageProcessorActor(RequestStore requestStore, CoreComponents coreComponents,
            List<MessageCheck<?, ?>> checks, CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader) {
        this.messageProcessor = new MessageProcessor(
                requestStore,
                coreComponents.statsEngine(),
                coreComponents.clock(),
                getSelf(), // Controller is self
                checks,
                correlationExtractor,
                useTimestampHeader);
    }

    // Constructor for testing
    public MessageProcessorActor(RequestStore requestStore, StatsEngine statsEngine,
            io.gatling.commons.util.Clock clock,
            List<MessageCheck<?, ?>> checks, CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader) {
        this.messageProcessor = new MessageProcessor(requestStore, statsEngine, clock, null, checks,
                correlationExtractor,
                useTimestampHeader);
    }

    public static Props props(RequestStore requestStore, CoreComponents coreComponents, List<MessageCheck<?, ?>> checks,
            CorrelationExtractor correlationExtractor, boolean useTimestampHeader) {
        // Ensure dependencies are non-null if required
        java.util.Objects.requireNonNull(requestStore, "RequestStore cannot be null");
        java.util.Objects.requireNonNull(coreComponents, "CoreComponents cannot be null");
        return Props.create(MessageProcessorActor.class,
                () -> new MessageProcessorActor(requestStore, coreComponents, checks, correlationExtractor,
                        useTimestampHeader));
    }

    public static Props props(RequestStore requestStore, CoreComponents coreComponents,
            List<MessageCheck<?, ?>> checks, String correlationHeaderName) {
        return props(requestStore, coreComponents, checks,
                new pl.perfluencer.kafka.extractors.HeaderExtractor(correlationHeaderName), false);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(List.class, list -> {
                    @SuppressWarnings("unchecked")
                    List<ConsumerRecord<String, Object>> records = (List<ConsumerRecord<String, Object>>) list;
                    messageProcessor.process(records);
                })
                .build();
    }
}
