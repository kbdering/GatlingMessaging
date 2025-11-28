package io.github.kbdering.kafka;

import io.github.kbdering.kafka.actors.KafkaProducerActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.apache.pekko.testkit.TestActorRef;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KafkaProducerActorTest {

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
    public void testSendMessage() {
        new TestKit(system) {
            {
                @SuppressWarnings("unchecked")
                org.apache.kafka.common.serialization.Serializer<Object> valueSerializer = (org.apache.kafka.common.serialization.Serializer<Object>) (org.apache.kafka.common.serialization.Serializer<?>) new StringSerializer();
                MockProducer<String, Object> producer = new MockProducer<>(true, new StringSerializer(),
                        valueSerializer);
                ActorRef actorRef = system.actorOf(KafkaProducerActor.props(producer));

                String topic = "test-topic";
                String key = "key";
                byte[] value = "value".getBytes();
                String correlationId = "corr-1";

                // Verify message was sent to producer
                try {
                    // Give some time for the actor to process
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Object message = "test-message";
                actorRef.tell(new KafkaProducerActor.ProduceMessage("test-topic", "key", message, correlationId),
                        getRef());

                // Verify actor sent back metadata (since auto-complete is true)
                expectMsgClass(RecordMetadata.class);

                assertEquals(1, producer.history().size());
                ProducerRecord<String, Object> record = producer.history().get(0);
                assertEquals("test-topic", record.topic());
                assertEquals("key", record.key());
                assertEquals(message, record.value());
            }
        };
    }

    @Test
    public void testSendMessageFailure() {
        new TestKit(system) {
            {
                @SuppressWarnings("unchecked")
                org.apache.kafka.common.serialization.Serializer<Object> valueSerializer = (org.apache.kafka.common.serialization.Serializer<Object>) (org.apache.kafka.common.serialization.Serializer<?>) new StringSerializer();
                MockProducer<String, Object> mockProducer = new MockProducer<>(false, new StringSerializer(),
                        valueSerializer);
                TestActorRef<KafkaProducerActor> producer = TestActorRef.create(system,
                        KafkaProducerActor.props(mockProducer));

                String topic = "test-topic";
                String key = "key";
                Object value = "value";
                String correlationId = "corr-1";

                producer.tell(new KafkaProducerActor.ProduceMessage(topic, key, value, correlationId), getRef());

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Fail the send
                mockProducer.errorNext(new RuntimeException("Kafka error"));

                // Verify actor sent back failure
                expectMsgClass(org.apache.pekko.actor.Status.Failure.class);
            }
        };
    }
}
