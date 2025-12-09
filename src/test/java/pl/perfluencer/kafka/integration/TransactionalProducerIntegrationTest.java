package pl.perfluencer.kafka.integration;

import pl.perfluencer.kafka.actions.KafkaRequestReplyActionBuilder;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import io.gatling.core.stats.StatsEngine;
import io.gatling.core.CoreComponents;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class TransactionalProducerIntegrationTest {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0");
    private static KafkaContainer kafka;
    private static ActorSystem system;

    @BeforeClass
    public static void setup() {
        kafka = new KafkaContainer(KAFKA_IMAGE);
        kafka.start();
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        if (kafka != null) {
            kafka.stop();
        }
    }

    @Test
    public void testTransactionalSend() throws Exception {
        String bootstrapServers = kafka.getBootstrapServers();
        String topic = "transactional-topic";
        String transactionalId = "my-tx-id";

        // 1. Configure Protocol with transactionalId
        KafkaProtocolBuilder protocol = KafkaDsl.kafka()
                .bootstrapServers(bootstrapServers)
                .transactionalId(transactionalId)
                .producerProperties(Collections.singletonMap(
                        org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        org.apache.kafka.common.serialization.StringSerializer.class.getName()))
                .numProducers(1)
                .numConsumers(0);

        // 2. Create Action
        StatsEngine statsEngine = mock(StatsEngine.class);
        // Pass null for CoreComponents as we modified KafkaProtocolComponents to handle
        // it
        CoreComponents coreComponents = null;

        // Build Action
        KafkaRequestReplyActionBuilder actionBuilder = KafkaDsl.kafkaRequestReply(
                "request", topic, "response-topic",
                s -> "key", s -> "value",
                pl.perfluencer.kafka.util.SerializationType.STRING,
                Collections.emptyList(),
                protocol.build(),
                5, TimeUnit.SECONDS);

        // We need to manually build the action to inject components, but the builder
        // uses internal Gatling logic.
        // Instead, let's just use the Protocol to create the components and verify the
        // actor starts without error.
        // And then manually send a message to the producer actor.

        KafkaProtocolBuilder.KafkaProtocol kafkaProtocol = protocol.build();
        KafkaProtocolBuilder.KafkaProtocolComponents components = new KafkaProtocolBuilder.KafkaProtocolComponents(
                kafkaProtocol, coreComponents);

        // Get the producer router
        org.apache.pekko.actor.ActorRef producerRouter = kafkaProtocol.getProducerRouter();

        // Send a message
        String correlationId = UUID.randomUUID().toString();
        pl.perfluencer.kafka.actors.KafkaProducerActor.ProduceMessage message = new pl.perfluencer.kafka.actors.KafkaProducerActor.ProduceMessage(
                topic, "key", "value", correlationId, true);

        TestKit probe = new TestKit(system);
        producerRouter.tell(message, probe.getRef());

        // Expect success response
        probe.expectMsgEquals(Duration.ofSeconds(10), "Message sent (transactional)");

        // 3. Verify message is committed using a read_committed consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            Assert.assertFalse("Should have received committed message", records.isEmpty());
            Assert.assertEquals("value", records.iterator().next().value());
        }
    }
}
