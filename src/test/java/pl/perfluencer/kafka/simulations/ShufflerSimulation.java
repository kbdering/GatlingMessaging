package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.javaapi.KafkaDsl.*;

public class ShufflerSimulation extends Simulation {

    static {
        pl.perfluencer.kafka.integration.TestConfig.init();
    }


    KafkaProtocolBuilder kafkaProtocol = kafka()
            .bootstrapServers(System.getProperty("kafka.bootstrap.servers", "localhost:9092"))
            .groupId("gatling-shuffler-audit-" + UUID.randomUUID().toString().substring(0, 8))
            .correlationByKey()
            .producerProperties(Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))
            .consumerProperties(Map.of(
                "auto.offset.reset", "earliest",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"
            ))
            .awaitConsumersReady(true)
            .consumerReadyTimeout(Duration.ofSeconds(60));

    ScenarioBuilder scn = scenario("Shuffler Integrity Audit")
            .exec(session -> {
                String correlationId = UUID.randomUUID().toString();
                String data = "shuffler-data-" + correlationId;
                return session.set("correlationId", correlationId).set("payload", data);
            })
            .pause(1)
            .exec(kafka("Direct Verification Audit")
                    .requestReply()
                    .requestTopic("Waiter-Requests")
                    .responseTopic("Shuffler-Responses")
                    .key(session -> session.getString("correlationId"))
                    .payload(session -> session.getString("payload"))
                    .timeout(Duration.ofSeconds(30))
                    .asString()
                    .check("Payload Match Check", (request, response) -> {
                        String reqId = request.replace("shuffler-data-", "");
                        if (response.contains(reqId)) {
                            return Optional.empty(); // Success
                        }
                        return Optional.of("DATA LEAKAGE DETECTED! Expected payload with ID: " + reqId + " but got: " + response);
                    }))
            .pause(60);

    {
        setUp(
                scn.injectOpen(atOnceUsers(100))
        ).protocols(kafkaProtocol).maxDuration(Duration.ofMinutes(2));
    }
}
