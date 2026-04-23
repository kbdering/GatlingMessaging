package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.kafka.integration.TestConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import pl.perfluencer.common.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

/**
 * Verification simulation for the Duplicator Service.
 * 
 * Flow:
 * 1. Send message to 'Waiter-Responses'
 * 2. Service duplicates it and sends to 'Duplicator-Responses'
 * 3. Gatling validates the original response and logs duplicates.
 */
public class DuplicatorSimulation extends Simulation {

    static {
        TestConfig.init();
    }

    private static final String BOOTSTRAP_SERVERS = System.getProperty(
            "kafka.bootstrap.servers", "localhost:9092");
    
    // Topics matching duplicator-service configuration
    private static final String REQUEST_TOPIC = "Waiter-Responses";
    private static final String RESPONSE_TOPIC = "Duplicator-Responses";

    {
        // Configure Kafka protocol
        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers(BOOTSTRAP_SERVERS)
                .groupId("gatling-duplicator-group-" + UUID.randomUUID().toString().substring(0, 8))
                .numProducers(1)
                .numConsumers(1)
                .pollTimeout(Duration.ofMillis(100))
                .correlationHeaderName("correlationId") // Required for request-reply matching
                .producerProperties(Map.of(
                        ProducerConfig.ACKS_CONFIG, "1",
                        ProducerConfig.LINGER_MS_CONFIG, "0"))
                .consumerProperties(Map.of(
                        "auto.offset.reset", "latest"));

        // Check to verify the duplicator correctly tagged the 'original' message
        MessageCheck<?, ?> originalTagCheck = new MessageCheck<>(
                "Original Tag Check",
                String.class, SerializationType.STRING,
                String.class, SerializationType.STRING,
                (String sent, String received) -> {
                    if (received != null && received.contains("[original]")) {
                        return Optional.empty();
                    }
                    return Optional.of("Message did not contain [original] prefix: " + received);
                });

        // Scenario: Send 1 request, expect 1 'original' response + duplicates
        ScenarioBuilder duplicatorScenario = scenario("Duplicator Verification Scenario")
                .exec(
                        KafkaDsl.kafka("Verify Duplication")
                                .requestReply()
                                .topics(REQUEST_TOPIC, RESPONSE_TOPIC)
                                .key(session -> UUID.randomUUID().toString())
                                .value(session -> ("Hello-Duplicator-" + UUID.randomUUID()).getBytes())
                                .asString()
                                .check(originalTagCheck)
                                .timeout(10, TimeUnit.SECONDS));

        // Setup: Ramp to 10 TPS for verification
        setUp(
                duplicatorScenario.injectOpen(
                        atOnceUsers(1),
                        rampUsersPerSec(1).to(10).during(10)
                )
        ).protocols(kafkaProtocol);
    }
}
