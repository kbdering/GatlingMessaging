package io.github.kbdering.kafka.integration;

import io.github.kbdering.kafka.actors.KafkaRawConsumerActor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class KafkaRawConsumeIntegrationTest {

    public static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        kafka.start();
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        kafka.stop();
    }

    @Test
    public void testRawConsume() throws ExecutionException, InterruptedException {
        new TestKit(system) {
            {
                String topic = "raw-consume-topic-" + UUID.randomUUID();
                String bootstrapServers = kafka.getBootstrapServers();

                // Produce a message
                Map<String, Object> producerProps = new HashMap<>();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
                producer.send(new ProducerRecord<>(topic, "key", "value")).get();
                producer.close();

                // Start Raw Consumer Actor
                Map<String, Object> consumerProps = new HashMap<>();
                consumerProps.put("bootstrap.servers", bootstrapServers);
                consumerProps.put("group.id", "raw-consumer-group");
                consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                consumerProps.put("auto.offset.reset", "earliest"); // Important to catch the message sent before

                ActorRef consumerActor = system
                        .actorOf(KafkaRawConsumerActor.props(consumerProps, topic, Duration.ofMillis(100)));

                // Request message
                consumerActor.tell(new KafkaRawConsumerActor.GetMessage(), getRef());

                // Expect message
                org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record = expectMsgClass(
                        org.apache.kafka.clients.consumer.ConsumerRecord.class);

                assertEquals("key", record.key());
                assertEquals("value", record.value());
            }
        };
    }
}
