package io.github.kbdering.kafka.integration;

import io.github.kbdering.kafka.actors.KafkaProducerActor;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CustomObjectSerializationTest {

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

    static class MyObject {
        public String name;
        public int value;

        public MyObject(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }

    static class MyObjectSerializer implements Serializer<MyObject> {
        @Override
        public byte[] serialize(String topic, MyObject data) {
            return (data.name + ":" + data.value).getBytes(StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testSendCustomObject() {
        new TestKit(system) {
            {
                @SuppressWarnings("unchecked")
                Serializer<Object> valueSerializer = (Serializer<Object>) (Serializer<?>) new MyObjectSerializer();
                MockProducer<String, Object> producer = new MockProducer<>(true, new StringSerializer(),
                        valueSerializer);
                ActorRef actorRef = system.actorOf(KafkaProducerActor.props(producer));

                String topic = "custom-obj-topic";
                String key = "key";
                MyObject message = new MyObject("test", 123);
                String correlationId = "corr-custom";

                actorRef.tell(new KafkaProducerActor.ProduceMessage(topic, key, message, correlationId), getRef());

                // Verify actor sent back metadata
                expectMsgClass(org.apache.kafka.clients.producer.RecordMetadata.class);

                assertEquals(1, producer.history().size());
                assertEquals(message, producer.history().get(0).value());

                // Verify serialization happened correctly in MockProducer (it stores the
                // object, but we can check if serializer works if we were using real producer)
                // MockProducer stores the value as passed.
                // But we can verify that the serializer we passed is compatible.
            }
        };
    }
}
