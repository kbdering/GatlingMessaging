package io.github.kbdering.kafka.actors;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import io.gatling.commons.stats.Status;
import io.gatling.core.CoreComponents;
import io.gatling.core.stats.StatsEngine;
import io.github.kbdering.kafka.cache.BatchProcessor;
import io.github.kbdering.kafka.cache.RequestStore;
import io.github.kbdering.kafka.MessageCheck;
import io.github.kbdering.kafka.extractors.CorrelationExtractor;
import io.github.kbdering.kafka.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import scala.Option; // Use Option explicitly
import scala.collection.immutable.List$; // For empty list

import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.HashMap;
import java.nio.charset.StandardCharsets;

public class MessageProcessorActor extends AbstractActor {

    private final RequestStore requestStore;
    private final StatsEngine statsEngine;
    private final List<MessageCheck<?, ?>> checks; // Use wildcard for generic list
    private CoreComponents coreComponents;
    private final CorrelationExtractor correlationExtractor; // Add extractor
    private final String correlationHeaderName;
    private final MessageProcessor messageProcessor;

    public MessageProcessorActor(RequestStore requestStore, CoreComponents coreComponents,
            List<MessageCheck<?, ?>> checks, CorrelationExtractor correlationExtractor, String correlationHeaderName) {
        this.requestStore = requestStore;
        this.statsEngine = coreComponents.statsEngine();
        this.checks = checks;
        this.coreComponents = coreComponents;
        this.correlationExtractor = correlationExtractor;
        this.correlationHeaderName = correlationHeaderName;
        this.messageProcessor = new MessageProcessor(requestStore, statsEngine, coreComponents.clock(), checks,
                correlationExtractor, correlationHeaderName);
    }

    // Constructor for testing
    public MessageProcessorActor(RequestStore requestStore, StatsEngine statsEngine,
            io.gatling.commons.util.Clock clock,
            List<MessageCheck<?, ?>> checks, CorrelationExtractor correlationExtractor, String correlationHeaderName) {
        this.requestStore = requestStore;
        this.statsEngine = statsEngine;
        this.checks = checks;
        this.correlationExtractor = correlationExtractor;
        this.coreComponents = null; // Not used when using this constructor
        this.correlationHeaderName = correlationHeaderName;
        this.messageProcessor = new MessageProcessor(requestStore, statsEngine, clock, checks, correlationExtractor,
                correlationHeaderName);
    }

    public static Props props(RequestStore requestStore, CoreComponents coreComponents, List<MessageCheck<?, ?>> checks,
            CorrelationExtractor correlationExtractor, String correlationHeaderName) {
        // Ensure dependencies are non-null if required
        java.util.Objects.requireNonNull(requestStore, "RequestStore cannot be null");
        java.util.Objects.requireNonNull(coreComponents, "CoreComponents cannot be null");
        return Props.create(MessageProcessorActor.class,
                () -> new MessageProcessorActor(requestStore, coreComponents, checks, correlationExtractor,
                        correlationHeaderName));
    }

    public static Props props(RequestStore requestStore, CoreComponents coreComponents,
            List<MessageCheck<?, ?>> checks, String correlationHeaderName) {
        return props(requestStore, coreComponents, checks, null, correlationHeaderName);
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
