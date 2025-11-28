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
        public final Object value; // Changed to Object
        public final String correlationId;

        public ProduceMessage(String topic, String key, Object value, String correlationId) {
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.correlationId = correlationId;
        }
    }

    public KafkaProducerActor(Map<String, Object> producerProperties) {
        this(new KafkaProducer<>(producerProperties));
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
        logger.info("Received ProduceMessage: topic={}, key={}, value={}", message.topic, message.key, message.value);
        ProducerRecord<String, Object> record = new ProducerRecord<>(message.topic, message.key, message.value);
        record.headers().add("correlationId", message.correlationId.getBytes());
        final org.apache.pekko.actor.ActorRef replyTo = getSender();
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
    }

    @Override
    public void postStop() {
        producer.close();
    }
}