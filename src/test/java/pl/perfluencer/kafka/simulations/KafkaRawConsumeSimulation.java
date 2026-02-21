package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.*;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.MessageCheck.*;

/**
 * Demonstrates raw message consumption using thread-based consumers.
 *
 * <p>
 * Consumer threads ({@code KafkaConsumerThread}) run in the background,
 * polling records and applying response-only checks via
 * {@code MessageProcessor.processConsumeOnly()}.
 * No request correlation is needed — every consumed record is validated
 * independently.
 * </p>
 *
 * <p>
 * The simulation first sends a batch of messages, then starts consuming
 * from the same topic with response-only checks.
 * </p>
 */
public class KafkaRawConsumeSimulation extends Simulation {

        private static final String BOOTSTRAP_SERVERS = System.getProperty(
                        "kafka.bootstrap.servers", "localhost:9092");
        private static final String TOPIC = System.getProperty(
                        "kafka.topic", "raw-consume-topic");

        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                        .bootstrapServers(BOOTSTRAP_SERVERS)
                        .groupId("raw-consumer-group-" + UUID.randomUUID().toString().substring(0, 8))
                        .numProducers(1)
                        .numConsumers(1)
                        .pollTimeout(Duration.ofMillis(100))
                        .consumerProperties(Map.of("auto.offset.reset", "earliest"));

        // Scenario 1: Send messages to the topic
        ScenarioBuilder sendScenario = scenario("Send Messages")
                        .exec(
                                        KafkaDsl.kafka(TOPIC,
                                                        session -> UUID.randomUUID().toString(),
                                                        session -> "raw-message-" + UUID.randomUUID()));

        // Scenario 2: Consume messages from the topic with response-only checks
        ScenarioBuilder consumeScenario = scenario("Consume Messages")
                        .exec(
                                        KafkaDsl.consume("Consume Raw", TOPIC)
                                                        .check(responseNotEmpty())
                                                        .check(responseContains("raw-message-"))
                                        .check(responseContains("THIS_SHOULD_FAIL")))
                        .pause(5); // Allow time for consumer threads to process

        {
                setUp(
                                sendScenario.injectOpen(atOnceUsers(5)),
                                consumeScenario.injectOpen(nothingFor(1), atOnceUsers(1)))
                                .protocols(kafkaProtocol);
        }
}
