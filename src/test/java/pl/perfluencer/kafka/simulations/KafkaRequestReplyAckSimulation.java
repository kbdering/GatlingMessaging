package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.kafka.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaRequestReplyAckSimulation extends Simulation {

    {
        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers("localhost:9092")
                .groupId("gatling-consumer-group-ack")
                .numProducers(1)
                .numConsumers(1)
                .producerProperties(Map.of(
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.LINGER_MS_CONFIG, "0"))
                .consumerProperties(Map.of(
                        "auto.offset.reset", "latest"));

        List<MessageCheck<?, ?>> checks = List.of(new MessageCheck<>(
                "Response Validation",
                String.class, SerializationType.STRING,
                String.class, SerializationType.STRING,
                (req, res) -> {
                    if (req.equals(res))
                        return Optional.empty();
                    return Optional.of("Mismatch: " + req + " != " + res);
                }));

        ScenarioBuilder scn = scenario("Kafka Request-Reply Ack Demo")
                .exec(
                        KafkaDsl.kafkaRequestReply("request_ack", "request_topic", "response_topic",
                                session -> UUID.randomUUID().toString(),
                                session -> "Hello Kafka " + UUID.randomUUID(),
                                SerializationType.STRING,
                                checks,
                                true, // waitForAck = true
                                10, TimeUnit.SECONDS));

        setUp(
                scn.injectOpen(constantUsersPerSec(1).during(5))).protocols(kafkaProtocol);
    }
}
