package pl.perfluencer.kafka.actors;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class KafkaRawConsumerActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRawConsumerActor.class);
    private static final Object POLL = new Object();

    private final org.apache.kafka.clients.consumer.Consumer<String, Object> consumer;
    private final Duration pollDuration;
    private final Queue<ConsumerRecord<String, Object>> messageBuffer = new LinkedList<>();
    private final Queue<org.apache.pekko.actor.ActorRef> pendingRequests = new LinkedList<>();

    public static class GetMessage {
    }

    private KafkaRawConsumerActor(Map<String, Object> consumerProperties, String topic, Duration pollDuration) {
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.pollDuration = pollDuration;
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    // Visible for testing
    public KafkaRawConsumerActor(org.apache.kafka.clients.consumer.Consumer<String, Object> consumer, String topic,
            Duration pollDuration) {
        this.consumer = consumer;
        this.pollDuration = pollDuration;
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public static Props props(Map<String, Object> consumerProperties, String topic, Duration pollDuration) {
        return Props.create(KafkaRawConsumerActor.class,
                () -> new KafkaRawConsumerActor(consumerProperties, topic, pollDuration));
    }

    public static Props props(org.apache.kafka.clients.consumer.Consumer<String, Object> consumer, String topic,
            Duration pollDuration) {
        return Props.create(KafkaRawConsumerActor.class,
                () -> new KafkaRawConsumerActor(consumer, topic, pollDuration));
    }

    @Override
    public void preStart() {
        self().tell(POLL, self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals(POLL, msg -> poll())
                .match(GetMessage.class, msg -> handleGetMessage())
                .build();
    }

    private void poll() {
        try {
            ConsumerRecords<String, Object> records = consumer.poll(pollDuration);
            for (ConsumerRecord<String, Object> record : records) {
                if (!pendingRequests.isEmpty()) {
                    org.apache.pekko.actor.ActorRef requestor = pendingRequests.poll();
                    requestor.tell(record, self());
                } else {
                    messageBuffer.offer(record);
                }
            }
            try {
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            } catch (Exception e) {
                logger.error("Error committing offsets", e);
            }
            self().tell(POLL, self());
        } catch (WakeupException e) {
            // Ignore
        } catch (Exception e) {
            logger.error("Error polling Kafka", e);
            getContext().system().scheduler().scheduleOnce(
                    Duration.ofSeconds(1),
                    self(),
                    POLL,
                    getContext().dispatcher(),
                    self());
        }
    }

    private void handleGetMessage() {
        if (!messageBuffer.isEmpty()) {
            getSender().tell(messageBuffer.poll(), self());
        } else {
            pendingRequests.offer(getSender());
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
