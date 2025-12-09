package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaSendSimulation extends Simulation {

        {
                KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                                .bootstrapServers("localhost:9092")
                                .groupId("gatling-consumer-group-send-test")
                                .numProducers(1) // Use 1 producer to verify reuse
                                .producerProperties(Map.of(
                                                ProducerConfig.ACKS_CONFIG, "all"));

                ScenarioBuilder scn = scenario("Kafka Send Simulation")
                                .exec(
                                                KafkaDsl.kafka("Req-Wait", "request_topic",
                                                                session -> UUID.randomUUID().toString(),
                                                                session -> "TestValue-Wait-"
                                                                                + UUID.randomUUID().toString(),
                                                                true, 30, java.util.concurrent.TimeUnit.SECONDS))
                                .exec(
                                                KafkaDsl.kafka("Req-Forget", "request_topic",
                                                                session -> UUID.randomUUID().toString(),
                                                                session -> "TestValue-Forget-"
                                                                                + UUID.randomUUID().toString(),
                                                                false, 30, java.util.concurrent.TimeUnit.SECONDS));

                setUp(
                                scn.injectOpen(
                                                constantUsersPerSec(10).during(10)))
                                .protocols(kafkaProtocol);
        }
}
