package io.github.kbdering.kafka;

import akka.actor.AbstractActor;
import akka.actor.Props;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;

import java.util.Map;


public class KafkaProducerActor extends AbstractActor {

    private final KafkaProducer<String, byte[]> producer;

    public static class ProduceMessage {
        public final String topic;
        public final String key;
        public final byte[] value; // Changed to byte[]
        public final String correlationId;

        public ProduceMessage(String topic, String key, byte[] value, String correlationId) {
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.correlationId = correlationId;
        }
    }

    private KafkaProducerActor(Map<String, Object> producerProperties) { // Made private to enforce props usage
        this.producer = new KafkaProducer<>(producerProperties);
    }

    public static Props props(Map<String, Object> producerProperties) {
        return Props.create(KafkaProducerActor.class, () -> new KafkaProducerActor(producerProperties));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ProduceMessage.class, this::handleProduceMessage)
                .build();
    }

    private void handleProduceMessage(ProduceMessage message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(message.topic, message.key, message.value);
        record.headers().add("correlationId", message.correlationId.getBytes());
        producer.send(record, (metadata, exception) -> {
                if (exception instanceof KafkaException){
                    System.err.println("Kafka Producer Error: " + exception.getMessage());
                }

        });
    }

    @Override
    public void postStop() {
        producer.close();
    }
}