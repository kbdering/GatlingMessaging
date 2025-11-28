package io.github.kbdering.kafka;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.github.kbdering.kafka.cache.RedisRequestStore;
import io.github.kbdering.kafka.cache.RequestStore;
import io.github.kbdering.kafka.javaapi.KafkaDsl;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import redis.clients.jedis.JedisPool;

// Import your dummy proto classes
import io.github.kbdering.kafka.proto.DummyRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaRequestReplyRedisSimulation extends Simulation {

    {
        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers("localhost:9092")
                .groupId("gatling-consumer-group-redis") // Use a unique group ID
                .numProducers(10)
                .numConsumers(3)
                .producerProperties(Map.of(
                        ProducerConfig.ACKS_CONFIG, "all"
                ))
                .consumerProperties(Map.of(
                        "auto.offset.reset", "latest",
                        "fetch.min.bytes", "1"
                ));

        // Configure Redis Request Store
        JedisPool jedisPool = new JedisPool("localhost", 6379);
        jedisPool.setMaxTotal(800);
        jedisPool.setMinIdle(400);
        jedisPool.setMaxIdle(400);
        RequestStore redisStore = new RedisRequestStore(jedisPool);
        kafkaProtocol.requestStore(redisStore);

        // Define MessageChecks
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
                    // Example check: verify echoed request_id
                    if (!deserializedRequest.getRequestId().equals(deserializedResponse.getRequestId())) {
                        return Optional.of("Protobuf response request_id '" + deserializedResponse.getRequestId() +
                                "' does not match original request_id '" + deserializedRequest.getRequestId() + "'");
                    }
                    return Optional.empty(); // Check passes
                }
        ));

        ScenarioBuilder scn = scenario("Kafka Request-Reply with RedisStore")
                .exec(
                        session -> session.set("myValueToSend", "TestValue-" + UUID.randomUUID().toString())
                )
                .exec(
                        KafkaDsl.kafkaRequestReply("request_topic", "request_topic",
                                session -> UUID.randomUUID().toString(),
                                session -> DummyRequest.newBuilder()
                                        .setRequestId(UUID.randomUUID().toString())
                                        .setRequestPayload(session.getString("myValueToSend"))
                                        .build().toByteArray(),
                                SerializationType.PROTOBUF,
                                sameRequestChecks,
                                10, TimeUnit.SECONDS)
                );

        setUp(
                scn.injectOpen(
                        nothingFor(Duration.ofSeconds(60)),
                        rampUsersPerSec(10).to(20000).during(60),
                        constantUsersPerSec(20000).during(120),
                        rampUsersPerSec(20000).to(10).during(60),
                        nothingFor(Duration.ofSeconds(60))
                )
        ).protocols(kafkaProtocol);
    }
}