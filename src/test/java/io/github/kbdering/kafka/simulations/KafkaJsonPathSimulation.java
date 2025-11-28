package io.github.kbdering.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.github.kbdering.kafka.javaapi.KafkaDsl;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import io.github.kbdering.kafka.util.SerializationType;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.nio.charset.StandardCharsets;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaJsonPathSimulation extends Simulation {

        {
                KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                                .bootstrapServers("localhost:9092")
                                .groupId("gatling-consumer-group-json")
                                .numProducers(1)
                                .numConsumers(1)
                                .correlationExtractor(KafkaDsl.jsonPath("$.correlationId")) // Use JSON Path extractor
                                .producerProperties(Map.of(
                                                ProducerConfig.ACKS_CONFIG, "all"));

                ScenarioBuilder scn = scenario("Kafka JSON Path Simulation")
                                .exec(
                                                KafkaDsl.kafkaRequestReply("request_topic", "response_topic",
                                                                session -> UUID.randomUUID().toString(), // Key (ignored
                                                                                                         // for
                                                                                                         // correlation)
                                                                session -> ("{\"correlationId\": \""
                                                                                + UUID.randomUUID().toString()
                                                                                + "\", \"data\": \"test\"}")
                                                                                .getBytes(StandardCharsets.UTF_8), // JSON
                                                                                                                   // Body
                                                                                                                   // as
                                                                                                                   // byte[]
                                                                SerializationType.STRING,
                                                                Collections.emptyList(),
                                                                10, TimeUnit.SECONDS));

                setUp(
                                scn.injectOpen(
                                                constantUsersPerSec(1).during(10)))
                                .protocols(kafkaProtocol);
        }
}
