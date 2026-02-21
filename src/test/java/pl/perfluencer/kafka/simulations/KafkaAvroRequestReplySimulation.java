/*
 * Copyright 2026 Perfluencer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.common.util.SerializationType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

/**
 * Basic Avro request-reply simulation using GenericRecord.
 *
 * <p>
 * Uses a simple "User" schema with name (string) and age (int) fields.
 * Requires a running Kafka broker and Schema Registry.
 * </p>
 *
 * <p>
 * Configure via system properties:
 * </p>
 * <ul>
 * <li>{@code kafka.bootstrap.servers} - default: localhost:9092</li>
 * <li>{@code schema.registry.url} - default: http://localhost:8081</li>
 * <li>{@code kafka.topic} - default: avro-test-topic</li>
 * </ul>
 */
public class KafkaAvroRequestReplySimulation extends Simulation {

        // Configuration
        private static final String BOOTSTRAP_SERVERS = System.getProperty(
                        "kafka.bootstrap.servers", "localhost:9092");
        private static final String SCHEMA_REGISTRY_URL = System.getProperty(
                        "schema.registry.url", "http://localhost:8081");
        private static final String TOPIC = System.getProperty(
                        "kafka.topic", "avro-test-topic");

        // Simple Avro schema: User { name: string, age: int }
        private static final String USER_SCHEMA_JSON = "{"
                        + "\"type\":\"record\","
                        + "\"name\":\"User\","
                        + "\"namespace\":\"pl.perfluencer.kafka.avro\","
                        + "\"fields\":["
                        + "  {\"name\":\"name\",\"type\":\"string\"},"
                        + "  {\"name\":\"age\",\"type\":\"int\"}"
                        + "]}";

        private static final Schema USER_SCHEMA = new Schema.Parser().parse(USER_SCHEMA_JSON);

        {
                RequestStore store = new InMemoryRequestStore();

                // Kafka protocol with Avro serializers
                KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                                .bootstrapServers(BOOTSTRAP_SERVERS)
                                .groupId("gatling-avro-group-" + UUID.randomUUID().toString().substring(0, 8))
                                .numProducers(3)
                                .numConsumers(3)
                                .pollTimeout(Duration.ofMillis(100))
                                .correlationHeaderName("correlationId")
                                .requestStore(store)
                                .producerProperties(Map.of(
                                                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                KafkaAvroSerializer.class.getName(),
                                                "schema.registry.url", SCHEMA_REGISTRY_URL))
                                .consumerProperties(Map.of(
                                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                                KafkaAvroDeserializer.class.getName(),
                                                "schema.registry.url", SCHEMA_REGISTRY_URL,
                                                "auto.offset.reset", "latest"));

                // Scenario
                ScenarioBuilder avroScenario = scenario("Avro Request-Reply")
                                .exec(
                                                KafkaDsl.kafka("Avro User Request")
                                                                .requestReply()
                                                                .topics(TOPIC, TOPIC)
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .value(session -> {
                                                                        GenericRecord user = new GenericData.Record(
                                                                                        USER_SCHEMA);
                                                                        user.put("name", "User-" + UUID.randomUUID()
                                                                                        .toString().substring(0, 8));
                                                                        user.put("age", (int) (Math.random() * 60)
                                                                                        + 18);
                                                                        return user;
                                                                })
                                                                .asAvro(GenericRecord.class, GenericRecord.class)
                                                                .check("Avro Name Check", (GenericRecord sent,
                                                                                GenericRecord received) -> {
                                                                        if (received == null)
                                                                                return Optional.of(
                                                                                                "Received Avro record was null");
                                                                        String sentName = sent.get("name").toString();
                                                                        String receivedName = received.get("name")
                                                                                        .toString();
                                                                        if (!sentName.equals(receivedName)) {
                                                                                return Optional.of(
                                                                                                "Name mismatch: sent='"
                                                                                                                + sentName
                                                                                                                + "' received='"
                                                                                                                + receivedName
                                                                                                                + "'");
                                                                        }
                                                                        return Optional.empty();
                                                                })
                                                                .timeout(10, TimeUnit.SECONDS));

                // Load profile: gentle ramp
                setUp(
                                avroScenario.injectOpen(
                                                nothingFor(Duration.ofSeconds(2)),
                                                rampUsersPerSec(1).to(10).during(10),
                                                constantUsersPerSec(10).during(30),
                                                nothingFor(Duration.ofSeconds(5))))
                                .protocols(kafkaProtocol);
        }
}
