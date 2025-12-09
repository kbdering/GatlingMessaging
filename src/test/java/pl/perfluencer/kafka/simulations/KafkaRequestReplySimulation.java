package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.cache.PostgresRequestStore;
import pl.perfluencer.kafka.cache.RequestStore;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import pl.perfluencer.kafka.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

// Import your dummy proto classes (assuming they are generated)
import pl.perfluencer.kafka.proto.DummyRequest;
import pl.perfluencer.kafka.proto.DummyResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaRequestReplySimulation extends Simulation {

    {
        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers("localhost:9092")
                .groupId("gatling-consumer-group-postgres")
                .numProducers(10)
                .numConsumers(3)
                .producerProperties(Map.of(
                        ProducerConfig.ACKS_CONFIG, "all"))
                .consumerProperties(Map.of(
                        "auto.offset.reset", "latest",
                        "fetch.min.bytes", "1"));

        // Example: SQL Pool using Hikari, Redis is also an option
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
        config.setUsername("postgres");
        config.setPassword("mysecretpassword");
        config.setDriverClassName("org.postgresql.Driver");
        config.setMaximumPoolSize(300);
        DataSource dataSource = new HikariDataSource(config);
        RequestStore postgresStore = new PostgresRequestStore(dataSource);
        kafkaProtocol.requestStore(postgresStore); // Use the builder's method

        // Define MessageChecks
        List<MessageCheck<?, ?>> stringChecks = new ArrayList<>();
        stringChecks.add(new MessageCheck<>(
                "Response String Value Check",
                String.class, SerializationType.STRING, // Expected type of stored request
                String.class, SerializationType.STRING, // Expected type of incoming response
                (String deserializedRequest, String deserializedResponse) -> {
                    if (deserializedRequest == null) {
                        return Optional.of("Stored request value was null after deserialization.");
                    }
                    if (deserializedResponse == null) {
                        return Optional.of("Received response value was null after deserialization.");
                    }
                    if (deserializedRequest.equals(deserializedResponse)) {
                        return Optional.empty(); // Check passes
                    } else {
                        return Optional.of("Response value '" + deserializedResponse
                                + "' does not match expected stored value '" + deserializedRequest + "'");
                    }
                }));
        List<MessageCheck<?, ?>> sameRequestChecks = new ArrayList<>();
        sameRequestChecks.add(new MessageCheck<DummyRequest, DummyRequest>(
                "Proto Response Check",
                DummyRequest.class, SerializationType.PROTOBUF,
                DummyRequest.class, SerializationType.PROTOBUF,
                (DummyRequest deserializedRequest, DummyRequest deserializedResponse) -> {
                    if (deserializedRequest == null) {
                        return Optional.of("Deserialized Protobuf request was null.");
                    }
                    if (deserializedResponse == null) {
                        return Optional.of("Deserialized Protobuf response was null.");
                    }
                    // Example check: verify echoed request_id and success status
                    if (!deserializedRequest.getRequestId().equals(deserializedResponse.getRequestId())) {
                        return Optional.of("Protobuf response request_id_echo '" + deserializedResponse.getRequestId() +
                                "' does not match original request_id '" + deserializedRequest.getRequestId() + "'");
                    }
                    return Optional.empty(); // Check passes
                }));

        List<MessageCheck<?, ?>> protoChecks = new ArrayList<>();
        protoChecks.add(new MessageCheck<>(
                "Proto Response Check",
                DummyRequest.class, SerializationType.PROTOBUF,
                DummyResponse.class, SerializationType.PROTOBUF,
                (DummyRequest deserializedRequest, DummyResponse deserializedResponse) -> {
                    if (deserializedRequest == null) {
                        return Optional.of("Deserialized Protobuf request was null.");
                    }
                    if (deserializedResponse == null) {
                        return Optional.of("Deserialized Protobuf response was null.");
                    }
                    // Example check: verify echoed request_id and success status
                    if (!deserializedRequest.getRequestId().equals(deserializedResponse.getRequestIdEcho())) {
                        return Optional.of("Protobuf response request_id_echo '"
                                + deserializedResponse.getRequestIdEcho() +
                                "' does not match original request_id '" + deserializedRequest.getRequestId() + "'");
                    }
                    if (!deserializedResponse.getSuccess()) {
                        return Optional.of("Protobuf response success field was false.");
                    }
                    return Optional.empty(); // Check passes
                }));

        ScenarioBuilder scn = scenario("Kafka Request-Reply with PostgresStore")
                .exec(
                        session -> session.set("myValueToSend", "TestValue-" + UUID.randomUUID().toString()))

                .exec(
                        KafkaDsl.kafkaRequestReply("request_topic", "request_topic",
                                session -> UUID.randomUUID().toString(),
                                session -> DummyRequest.newBuilder()
                                        .setRequestId(UUID.randomUUID().toString())
                                        .setRequestPayload(session.getString("myValueToSend"))
                                        .build().toByteArray(),
                                SerializationType.PROTOBUF,
                                sameRequestChecks, // Pass the list of checks
                                10, TimeUnit.SECONDS)

                );

        setUp(
                scn.injectOpen(
                        nothingFor(Duration.ofSeconds(60)), // a synchronisation needed for the consumer group metadata
                                                            // retrival and rebalancing
                        rampUsersPerSec(10).to(20000).during(60),
                        constantUsersPerSec(20000).during(120),
                        rampUsersPerSec(20000).to(10).during(60),
                        nothingFor(Duration.ofSeconds(60)) // also a sync point required - the messages in flight could
                                                           // be lost. for now keep it at least the timeout time.
                )).protocols(kafkaProtocol);
    }
}