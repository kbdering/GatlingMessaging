package io.github.kbdering.kafka;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.gatling.commons.stats.Status;
import io.gatling.core.stats.StatsEngine;
import io.github.kbdering.kafka.cache.RequestStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import io.gatling.core.CoreComponents;
import scala.collection.immutable.List$;

public class KafkaConsumerActor extends AbstractActor {

    private final KafkaConsumer<String, String> consumer;
    private final RequestStore requestStore;
    private static CoreComponents coreComponents = null;


    public KafkaConsumerActor(Map<String, Object> consumerProperties, String responseTopic,  RequestStore requestStore, CoreComponents coreComponents) {
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.requestStore = requestStore;
        this.coreComponents = coreComponents;
        consumer.subscribe(Collections.singletonList(responseTopic));

    }

    public static Props props(Map<String, Object> consumerProperties, String responseTopic, RequestStore requestStore, CoreComponents coreComponents) {
        return Props.create(KafkaConsumerActor.class, () -> new KafkaConsumerActor(consumerProperties,  responseTopic, requestStore, coreComponents));
    }
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchAny(this::consume)
                .build();
    }

    private void consume(Object message) {
        final StatsEngine statsEngine = coreComponents.statsEngine();

        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                long endTime = coreComponents.clock().nowMillis();
                for (ConsumerRecord<String, String> record : records) {
                    String correlationId = null;
                    if (record.headers().lastHeader("correlationId") != null) {
                        correlationId = new String(record.headers().lastHeader("correlationId").value());
                    }

                    if (correlationId != null) {
                        long startTime = endTime;
                        try {

                            Map<String, String> requestData = requestStore.getRequest(record.key());
                            if (requestData != null && !requestData.isEmpty()) {
                                startTime = Long.parseLong(requestData.get("startTime"));
                                String transactionName = requestData.get("transactionName");

                                statsEngine.logResponse("kafka-consumer ", List$.MODULE$.empty(), transactionName, startTime, endTime, Status.apply("OK"),
                                        scala.Some.apply("500"), scala.Some.apply(""));

                                requestStore.deleteRequest(correlationId);
                            } else {
                                statsEngine.logResponse("kafka-consumer ", List$.MODULE$.empty(), "Missing Match", startTime, endTime, Status.apply("OK"),
                                        scala.Some.apply("500"), scala.Some.apply("Request data not found for correlationId: " + correlationId));
                                System.err.println("Request data not found for correlationId: " + correlationId);
                            }
                        } catch (Exception e) {
                            System.err.println("Error retrieving from storage or processing: " + e.getMessage());
                            statsEngine.logResponse("kafka-consumer ", List$.MODULE$.empty(), "Missing Match", startTime, endTime, Status.apply("OK"),
                                    scala.Some.apply("500"), scala.Some.apply("Error processing request/repsonse: " + correlationId + " message : " + e.getMessage()));

                        }
                    } else {
                        System.err.println("Received message without correlation ID. Ignoring.");
                    }
                }
                consumer.commitAsync();
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