package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;

/**
 * Demonstrates generating a payload, storing it in the Gatling Session,
 * and using Gatling EL "#{sessionVariable}" to pass it to the Kafka builder.
 */
public class KafkaSessionVariableSimulation extends Simulation {

    private static final String BOOTSTRAP_SERVERS = System.getProperty(
            "kafka.bootstrap.servers", "localhost:9092"); // Use a standard default for CI

    private static final String TOPIC = "session-variable-topic";

    // ==================== Protocol ====================

    KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .numProducers(1)
            .producerProperties(Map.of(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

    // ==================== Scenario ====================

    ScenarioBuilder sessionScenario = scenario("Session Variable Flow")
            .exec(session -> {
                // Generate a random payload and store it in the session
                String randomPayload = "{\"id\": \"" + UUID.randomUUID().toString() + "\", \"type\": \"NEW_ORDER\"}";
                return session.set("myPayload", randomPayload);
            })
            .exec(
                    KafkaDsl.kafka("Send Payload from Session")
                            .send()
                            .topic(TOPIC)
                            .key(session -> UUID.randomUUID().toString())
                            // Use Gatling EL string instead of a Function!
                            .value("#{myPayload}"))
            .pause(1)
            .exec(session -> {
                // Generate a random correlation ID and header and store them in the session
                String correlationId = UUID.randomUUID().toString();
                String someHeader = "debug-mode";
                return session.set("myCorrelationId", correlationId).set("myHeader", someHeader);
            })
            .exec(
                    KafkaDsl.kafka("Send with EL key and header")
                            .send()
                            .topic(TOPIC)
                            // Use Gatling EL strings for key and headers as well
                            .key("#{myCorrelationId}")
                            .value("{\"status\": \"CHECK\"}")
                            .header("X-Custom-Header", "#{myHeader}"));

    // ==================== Simulation Pipeline ====================

    {
        setUp(
                sessionScenario.injectOpen(atOnceUsers(1)).protocols(kafkaProtocol))
                .maxDuration(Duration.ofSeconds(10));
    }
}
