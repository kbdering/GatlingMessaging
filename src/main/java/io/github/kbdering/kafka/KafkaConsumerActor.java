package io.github.kbdering.kafka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer; // Import ByteArrayDeserializer

import java.time.Duration; // Use java.time.Duration
import java.util.Collections; // Use java.util.Collections
import java.util.Map; // Use java.util.Map
import java.util.HashMap;


import io.gatling.core.CoreComponents;

public class KafkaConsumerActor extends AbstractActor {

    private final KafkaConsumer<String, byte[]> consumer; // Changed to byte[]
    private final ActorRef messageProcessorRouter;
    private final CoreComponents coreComponents; // Keep coreComponents to access clock

    private KafkaConsumerActor(Map<String, Object> consumerProperties, String responseTopic, ActorRef messageProcessorRouter, CoreComponents coreComponents) {
        // Ensure consumer properties are set for byte array deserialization
        Map<String, Object> updatedConsumerProps = new java.util.HashMap<>(consumerProperties);
        updatedConsumerProps.putIfAbsent(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(updatedConsumerProps);
        this.messageProcessorRouter = messageProcessorRouter;
        this.coreComponents = coreComponents;
        consumer.subscribe(Collections.singletonList(responseTopic));

    }

    public static Props props(Map<String, Object> consumerProperties, String responseTopic, ActorRef messageProcessorRouter, CoreComponents coreComponents) {
        return Props.create(KafkaConsumerActor.class, () -> new KafkaConsumerActor(consumerProperties, responseTopic, messageProcessorRouter, coreComponents));
    }
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchAny(this::consume)
                .build();
    }

    private void consume(Object message) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100)); // Changed to byte[]
                if (records.isEmpty()) {
                    continue;
                }
                // Convert records to a Map<String, ConsumerRecord> to preserve headers
                Map<String, byte[]> messageMap = new HashMap<>();
                for (ConsumerRecord<String, byte[]> record : records) {
                    messageMap.put(record.key(), record.value());
                }
                this.messageProcessorRouter.tell(messageMap, getSelf());
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public void postStop() {
        consumer.close();
    }
}