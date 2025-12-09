package pl.perfluencer.kafka;

import pl.perfluencer.kafka.actors.KafkaConsumerActor;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.kafka.clients.consumer.MockConsumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.pekko.actor.ActorRef;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerActorTest {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testPoll() {
        new TestKit(system) {
            {
                String topic = "test-topic";
                MockConsumer<String, Object> mockConsumer = new MockConsumer<>(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);
                TopicPartition tp = new TopicPartition(topic, 0);
                mockConsumer.schedulePollTask(() -> {
                    mockConsumer.rebalance(Collections.singletonList(tp));
                    mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
                    mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 0, "key",
                            "value"));
                });

                Map<String, Object> props = new HashMap<>();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("group.id", "test-group");

                ActorRef consumer = system
                        .actorOf(KafkaConsumerActor.props(mockConsumer, topic, getRef(), null, Duration.ofMillis(100)));

                // The actor automatically polls on preStart, so we should expect a message
                // The message sent to the router (getRef()) is a Map<String, byte[]>

                expectMsgClass(java.util.List.class);
            }
        };
    }

    @Test
    public void testPollFailure() {
        new TestKit(system) {
            {
                String topic = "test-topic";
                MockConsumer<String, Object> mockConsumer = new MockConsumer<>(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);
                mockConsumer.schedulePollTask(() -> {
                    throw new org.apache.kafka.common.KafkaException("Poll failed");
                });

                Map<String, Object> props = new HashMap<>();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("group.id", "test-group");

                ActorRef consumer = system
                        .actorOf(KafkaConsumerActor.props(mockConsumer, topic, getRef(), null, Duration.ofMillis(100)));

                // The actor logs the error and continues polling. It does NOT crash or stop.
                // So we can't easily verify the error unless we check logs or side effects.
                // However, we can verify it doesn't send anything to the router.

                expectNoMessage(Duration.ofMillis(200));
            }
        };
    }
}
