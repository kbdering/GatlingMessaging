package pl.perfluencer.kafka.actors;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.AuthorizationException;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.gatling.core.stats.StatsEngine;
import io.gatling.commons.util.Clock;
import io.gatling.commons.stats.Status;
import scala.Option;
import scala.collection.immutable.List;

public class KafkaProducerActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerActor.class);
    private final Producer<String, Object> producer;

    public static class ProduceMessage {
        public final String topic;
        public final String key;
        public final Object value;
        public final String correlationId;
        public final Map<String, String> headers;
        public final String correlationHeaderName;
        public final boolean waitForAck;
        public final String requestName;
        public final String scenario;
        public final List<String> groups;

        public ProduceMessage(String topic, String key, Object value, String correlationId, boolean waitForAck,
                String requestName, String scenario, List<String> groups) {
            this(topic, key, value, correlationId, java.util.Collections.emptyMap(), "correlationId", waitForAck,
                    requestName, scenario, groups);
        }

        public ProduceMessage(String topic, String key, Object value, String correlationId, Map<String, String> headers,
                String correlationHeaderName, boolean waitForAck, String requestName, String scenario,
                List<String> groups) {
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.correlationId = correlationId;
            this.headers = headers;
            this.correlationHeaderName = correlationHeaderName;
            this.waitForAck = waitForAck;
            this.requestName = requestName;
            this.scenario = scenario;
            this.groups = groups;
        }
    }

    private final boolean isTransactional;
    private final StatsEngine statsEngine;
    private final Clock clock;

    public KafkaProducerActor(Map<String, Object> producerProperties, StatsEngine statsEngine, Clock clock) {
        logger.info("Starting KafkaProducerActor with properties: {}", producerProperties);
        this.isTransactional = producerProperties.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        this.statsEngine = statsEngine;
        this.clock = clock;
        try {
            this.producer = new KafkaProducer<>(producerProperties);
            if (isTransactional) {
                logger.info("Initializing Kafka transactions for producer: {}",
                        producerProperties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
                this.producer.initTransactions();
            }
        } catch (KafkaException e) {
            logger.error("Failed to create Kafka Producer. Check your bootstrap.servers configuration.", e);
            throw e;
        }
    }

    // Visible for testing
    public KafkaProducerActor(Producer<String, Object> producer, StatsEngine statsEngine, Clock clock) {
        this.producer = producer;
        this.isTransactional = false;
        this.statsEngine = statsEngine;
        this.clock = clock;
    }

    public static Props props(Map<String, Object> producerProperties, StatsEngine statsEngine, Clock clock) {
        return Props.create(KafkaProducerActor.class,
                () -> new KafkaProducerActor(producerProperties, statsEngine, clock));
    }

    // Visible for testing
    public static Props props(Producer<String, Object> producer, StatsEngine statsEngine, Clock clock) {
        return Props.create(KafkaProducerActor.class, () -> new KafkaProducerActor(producer, statsEngine, clock));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ProduceMessage.class, this::handleProduceMessage)
                .build();
    }

    private void handleProduceMessage(ProduceMessage message) {
        long start = clock != null ? clock.nowMillis() : System.currentTimeMillis();
        logger.debug("Received ProduceMessage: topic={}, key={}, value={}, waitForAck={}", message.topic, message.key,
                message.value, message.waitForAck);
        ProducerRecord<String, Object> record = new ProducerRecord<>(message.topic, message.key, message.value);

        if (message.correlationId != null) {
            record.headers().add(message.correlationHeaderName, message.correlationId.getBytes());
        }

        if (message.headers != null) {
            for (Map.Entry<String, String> entry : message.headers.entrySet()) {
                if (entry.getValue() != null) {
                    record.headers().add(entry.getKey(), entry.getValue().getBytes());
                }
            }
        }

        final org.apache.pekko.actor.ActorRef replyTo = getSender();

        if (isTransactional) {
            try {
                producer.beginTransaction();
                producer.send(record);
                producer.commitTransaction();
                // For transactional, we assume success if commit succeeds.
                // We can't get RecordMetadata easily without a callback, but commit guarantees
                // persistence.
                // We could use a callback to get metadata but commit is the barrier.
                // Let's use a callback to capture metadata, but we must be careful.
                // Actually, send() returns a Future.
                // But we are in an actor, we shouldn't block... wait, commitTransaction IS
                // blocking.
                // So we are already blocking.
                // Let's just return a success message.
                replyTo.tell("Message sent (transactional)", getSelf());
            } catch (ProducerFencedException | org.apache.kafka.common.errors.OutOfOrderSequenceException
                    | org.apache.kafka.common.errors.AuthorizationException e) {
                // We can't recover from these exceptions, so we close the producer and rethrow
                producer.close();
                replyTo.tell(new org.apache.pekko.actor.Status.Failure(e), getSelf());
                throw e;
            } catch (KafkaException e) {
                // For all other exceptions, abort the transaction and try to retry?
                // The actor will restart if we throw.
                producer.abortTransaction();
                replyTo.tell(new org.apache.pekko.actor.Status.Failure(e), getSelf());
                // We don't rethrow here to avoid restarting the actor for transient errors?
                // Actually, if we abort, we are ready for next transaction.
                logger.error("Transaction aborted", e);
            }
        } else if (message.waitForAck) {
            producer.send(record, (metadata, exception) -> {
                long end = clock != null ? clock.nowMillis() : System.currentTimeMillis();
                logMetrics(message, start, end, exception);
                if (exception != null) {
                    replyTo.tell(new org.apache.pekko.actor.Status.Failure(exception), getSelf());
                    if (exception instanceof KafkaException) {
                        logger.error("Kafka Producer Error", exception);
                    }
                } else {
                    replyTo.tell(metadata, getSelf());
                }
            });
        } else {
            // Fire and forget - send immediately and don't wait for callback to reply
            // We still use a callback to log errors, but we reply to the sender immediately
            producer.send(record, (metadata, exception) -> {
                long end = clock != null ? clock.nowMillis() : System.currentTimeMillis();
                logMetrics(message, start, end, exception);
                if (exception != null) {
                    logger.error("Kafka Producer Error (Async)", exception);
                }
            });
            // Reply with a placeholder or null since we don't have metadata yet
            // Actually, for fire-and-forget, the Action might not even expect a reply,
            // but keeping the protocol consistent is good.
            // However, if we reply immediately, we can't send RecordMetadata.
            // Let's send a success object.
            replyTo.tell("Message sent (async)", getSelf());
        }
    }

    private void logMetrics(ProduceMessage message, long start, long end, Exception exception) {
        if (statsEngine != null && message.requestName != null) {
            Status status = exception == null ? Status.apply("OK") : Status.apply("KO");
            Option<String> messageOpt = exception == null ? Option.empty() : Option.apply(exception.getMessage());
            statsEngine.logResponse(
                    message.scenario,
                    message.groups,
                    message.requestName + " Send",
                    start,
                    end,
                    status,
                    Option.empty(),
                    messageOpt);
        }
    }

    @Override
    public void postStop() {
        producer.close();
    }
}
