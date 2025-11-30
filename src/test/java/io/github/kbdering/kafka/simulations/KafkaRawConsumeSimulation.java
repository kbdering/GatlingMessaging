package io.github.kbdering.kafka.simulations;

import io.gatling.javaapi.core.*;
import io.github.kbdering.kafka.javaapi.KafkaDsl;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;

import java.time.Duration;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaRawConsumeSimulation extends Simulation {

    KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
            .bootstrapServers("localhost:9092")
            .groupId("raw-consumer-group")
            .pollTimeout(Duration.ofMillis(100));

    ScenarioBuilder scn = scenario("Raw Consume Scenario")
            .exec(KafkaDsl.kafka("Send Message", "raw-topic", "key", "value"))
            .pause(1)
            .exec(KafkaDsl.consume("Consume Message", "raw-topic"));

    {
        setUp(
                scn.injectOpen(atOnceUsers(1))).protocols(kafkaProtocol);
    }
}
