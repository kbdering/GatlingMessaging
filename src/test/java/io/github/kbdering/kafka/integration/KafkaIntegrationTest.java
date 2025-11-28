package io.github.kbdering.kafka.integration;

import io.github.kbdering.kafka.actors.KafkaConsumerActor;
import io.github.kbdering.kafka.actors.KafkaProducerActor;
import io.github.kbdering.kafka.util.SerializationType;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@Ignore("Docker API compatibility issue. Install Testcontainers Desktop (https://testcontainers.com/desktop/) to run locally.")
public class KafkaIntegrationTest {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0");
    private static KafkaContainer kafka;
    private static ActorSystem system;

    @BeforeClass
    public static void setUp() throws ExecutionException, InterruptedException {
        kafka = new KafkaContainer(KAFKA_IMAGE);
        kafka.start();

        createTopic("test-topic");
        createTopic("response-topic");

        system = ActorSystem.create("KafkaIntegrationTestSystem");
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system,
                scala.concurrent.duration.Duration.create(10, java.util.concurrent.TimeUnit.SECONDS), true);
        kafka.stop();
    }

    private static void createTopic(String topicName) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }
    }

    @Test
    public void testEndToEndFlow() {
        TestKit probe = new TestKit(system);
        String requestTopic = "test-topic";
        String responseTopic = "response-topic";
        String correlationId = "corr-123";
        String key = "key-1";
        String value = "value-1";

        // 1. Setup Consumer Actor (simulating the Gatling Kafka Extension's consumer)
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "gatling-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ActorRef consumerActor = system.actorOf(
                KafkaConsumerActor.props(consumerProps, responseTopic, probe.getRef(), null, Duration.ofMillis(100)));

        // 2. Setup Producer Actor (simulating the Gatling Kafka Extension's producer)
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        ActorRef producerActor = system.actorOf(KafkaProducerActor.props(producerProps));

        // 3. Send message to Request Topic
        producerActor.tell(new KafkaProducerActor.ProduceMessage(requestTopic, key, value.getBytes(), correlationId),
                probe.getRef());

        // 4. Simulate Application Under Test: Produce response to Response Topic
        try (org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> appProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(
                producerProps)) {
            org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> responseRecord = new org.apache.kafka.clients.producer.ProducerRecord<>(
                    responseTopic, key, "response-value".getBytes());
            responseRecord.headers().add("correlationId", correlationId.getBytes());
            appProducer.send(responseRecord).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 5. Verify Consumer Actor receives the message
        // receiveOne in javadsl takes java.time.Duration
        Object msg = probe.receiveOne(Duration.ofSeconds(10));
        if (msg instanceof java.util.Map) {
            java.util.Map<?, ?> map = (java.util.Map<?, ?>) msg;
            // Success
        } else {
            throw new AssertionError("Expected Map, got: " + msg);
        }
    }
}
