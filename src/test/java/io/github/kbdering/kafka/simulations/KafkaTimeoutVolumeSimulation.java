package io.github.kbdering.kafka.simulations;

import io.gatling.javaapi.core.*;
import io.github.kbdering.kafka.javaapi.KafkaDsl;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaTimeoutVolumeSimulation extends Simulation {

        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String topic = "timeout-volume-topic";

        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                        .bootstrapServers(bootstrapServers)
                        .groupId("timeout-group")
                        .timeoutCheckInterval(Duration.ofMillis(100)) // Check every 100ms
                        .producerProperties(Map.of(
                                        ProducerConfig.ACKS_CONFIG, "1",
                                        ProducerConfig.LINGER_MS_CONFIG, "0"));

        ScenarioBuilder scn = scenario("Timeout Volume Scenario")
                        .exec(
                                        KafkaDsl.kafkaRequestReply("Timeout Request", topic, "reply-topic",
                                                        session -> "key",
                                                        "value",
                                                        null,
                                                        500, java.util.concurrent.TimeUnit.MILLISECONDS))
                        .pause(1);

        {
                setUp(
                                scn.injectOpen(
                                                rampUsersPerSec(10).to(100).during(10), // Ramp up to 100 req/s
                                                constantUsersPerSec(100).during(10) // Hold for 10s
                                )).protocols(kafkaProtocol);
        }
}
