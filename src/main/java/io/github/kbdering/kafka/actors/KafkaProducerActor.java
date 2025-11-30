package io.github.kbdering.kafka.actors;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerActor.class);
    private final Producer<String, Object> producer;

    public static class ProduceMessage {
        public final String topic;
        public final String key;
        public final Object value;
        public final String correlationId;
        public final boolean waitForAck;

        public ProduceMessage(String topic, String key, Object value, String correlationId, boolean waitForAck) {
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.correlationId = correlationId;
            this.waitForAck = waitForAck;
        }
    }

    public KafkaProducerActor(Map<String, Object> producerProperties) {
        logger.info("Starting KafkaProducerActor with properties: {}", producerProperties);
        try {
            this.producer = new KafkaProducer<>(producerProperties);
        } catch (KafkaException e) {
            logger.error("Failed to create Kafka Producer. Check your bootstrap.servers configuration.", e);
            throw e;
        }
    }

    // Visible for testing
    public KafkaProducerActor(Producer<String, Object> producer) {
        this.producer = producer;
    }

    public static Props props(Map<String, Object> producerProperties) {
        return Props.create(KafkaProducerActor.class, () -> new KafkaProducerActor(producerProperties));
    }

    // Visible for testing
    public static Props props(Producer<String, Object> producer) {
        return Props.create(KafkaProducerActor.class, () -> new KafkaProducerActor(producer));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ProduceMessage.class, this::handleProduceMessage)
                .build();
    }

    private void handleProduceMessage(ProduceMessage message) {
        logger.debug("Received ProduceMessage: topic={}, key={}, value={}, waitForAck={}", message.topic, message.key,
                message.value, message.waitForAck);
        ProducerRecord<String, Object> record = new ProducerRecord<>(message.topic, message.key, message.value);
        if (message.correlationId != null) {
            record.headers().add("correlationId", message.correlationId.getBytes());
        }

        final org.apache.pekko.actor.ActorRef replyTo = getSender();

        if (message.waitForAck) {
            producer.send(record, (metadata, exception) -> {
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

    @Override
    public void postStop() {
        producer.close();
    }
}