package io.github.kbdering.kafka.simulations;

import io.gatling.javaapi.core.*;
import io.github.kbdering.kafka.javaapi.KafkaDsl;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;

import java.time.Duration;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaRawConsumeSimulation extends Simulation {

    KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
            .bootstrapServers(System.getProperty("kafka.bootstrap.servers", "localhost:9092"))
            .groupId("raw-consumer-group")
            .pollTimeout(Duration.ofMillis(100))
            .consumerProperties(java.util.Map.of("auto.offset.reset", "earliest"));

    ScenarioBuilder scn = scenario("Raw Consume Scenario")
            .exec(KafkaDsl.kafka("Send Message", "raw-topic", "key", "value"))
            .pause(1)
            .exec(KafkaDsl.consume("Consume Message", "raw-topic").saveAs("consumedMessage"))
            .exec(session -> {
                String message = session.getString("consumedMessage");
                if (message == null) {
                    throw new RuntimeException("Message not found in session");
                }
                if (!"value".equals(message)) {
                    throw new RuntimeException("Unexpected message: " + message);
                }
                return session;
            });

    {
        setUp(
                scn.injectOpen(atOnceUsers(1))).protocols(kafkaProtocol);
    }
}
