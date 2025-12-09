package pl.perfluencer.kafka.actors;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.gatling.core.CoreComponents;

public class KafkaConsumerActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerActor.class);
    private static final Object POLL = new Object();

    private final org.apache.kafka.clients.consumer.Consumer<String, Object> consumer;
    private final ActorRef messageProcessorRouter;
    private final Duration pollDuration;

    private KafkaConsumerActor(Map<String, Object> consumerProperties, String responseTopic,
            ActorRef messageProcessorRouter, CoreComponents coreComponents, Duration pollDuration) {
        Map<String, Object> updatedConsumerProps = new HashMap<>(consumerProperties);

        logger.info("Starting KafkaConsumerActor for topic: {} with group: {}", responseTopic,
                updatedConsumerProps.get("group.id"));
        if (!updatedConsumerProps.containsKey("auto.offset.reset")) {
            logger.warn(
                    "auto.offset.reset is not set. Defaulting to 'latest' which may cause missed messages in tests.");
        }

        try {
            this.consumer = new KafkaConsumer<>(updatedConsumerProps);
        } catch (Exception e) {
            logger.error("Failed to create Kafka Consumer. Check your bootstrap.servers and group.id configuration.",
                    e);
            throw e;
        }

        this.messageProcessorRouter = messageProcessorRouter;
        this.pollDuration = pollDuration;
        consumer.subscribe(Collections.singletonList(responseTopic));
    }

    // Visible for testing
    public KafkaConsumerActor(org.apache.kafka.clients.consumer.Consumer<String, Object> consumer, String responseTopic,
            ActorRef messageProcessorRouter, CoreComponents coreComponents, Duration pollDuration) {
        this.consumer = consumer;
        this.messageProcessorRouter = messageProcessorRouter;
        this.pollDuration = pollDuration;
        consumer.subscribe(Collections.singletonList(responseTopic));
    }

    public static Props props(Map<String, Object> consumerProperties, String responseTopic,
            ActorRef messageProcessorRouter, CoreComponents coreComponents, Duration pollDuration) {
        return Props.create(KafkaConsumerActor.class, () -> new KafkaConsumerActor(consumerProperties, responseTopic,
                messageProcessorRouter, coreComponents, pollDuration));
    }

    // Visible for testing
    public static Props props(org.apache.kafka.clients.consumer.Consumer<String, Object> consumer, String responseTopic,
            ActorRef messageProcessorRouter, CoreComponents coreComponents, Duration pollDuration) {
        return Props.create(KafkaConsumerActor.class, () -> new KafkaConsumerActor(consumer, responseTopic,
                messageProcessorRouter, coreComponents, pollDuration));
    }

    @Override
    public void preStart() {
        self().tell(POLL, self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(POLL, msg -> poll())
                .build();
    }

    private void poll() {
        try {
            ConsumerRecords<String, Object> records = consumer.poll(pollDuration);
            if (!records.isEmpty()) {
                List<ConsumerRecord<String, Object>> recordList = new ArrayList<>();
                for (ConsumerRecord<String, Object> record : records) {
                    recordList.add(record);
                }
                this.messageProcessorRouter.tell(recordList, self());
                try {
                    consumer.commitSync();
                } catch (Exception e) {
                    logger.error("Error committing offsets", e);
                }
            }
            self().tell(POLL, self());
        } catch (WakeupException e) {
            // Ignore exception if closing
        } catch (Exception e) {
            logger.error("Error polling Kafka", e);
            // Add a backoff delay to prevent tight loop on error
            getContext().system().scheduler().scheduleOnce(
                    Duration.ofSeconds(1),
                    self(),
                    POLL,
                    getContext().dispatcher(),
                    self());
        }
    }

    @Override
    public void postStop() {
        try {
            consumer.wakeup();
            consumer.close();
        } catch (Exception e) {
            logger.error("Error closing Kafka consumer", e);
        }
    }
}