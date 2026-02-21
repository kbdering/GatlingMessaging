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
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

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

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.MessageCheck.*;

/**
 * Demonstrates Avro send + consume-only using Schema Registry.
 *
 * <p>
 * Sends Avro-encoded {@code User} GenericRecords via the fluent fire-and-forget
 * DSL ({@code kafka("...").send().asAvro()}) with Confluent Schema Registry,
 * then consumes them in raw mode with typed GenericRecord checks.
 * </p>
 *
 * <p>
 * Configure via system properties:
 * </p>
 * <ul>
 * <li>{@code kafka.bootstrap.servers} - default: 192.168.1.143:9094</li>
 * <li>{@code schema.registry.url} - default: http://localhost:8081</li>
 * <li>{@code kafka.topic} - default: avro-consume-topic</li>
 * </ul>
 */
public class KafkaAvroSchemaRegistrySimulation extends Simulation {

        private static final String BOOTSTRAP_SERVERS = System.getProperty(
                        "kafka.bootstrap.servers", "192.168.1.143:9094");
        private static final String SCHEMA_REGISTRY_URL = System.getProperty(
                        "schema.registry.url", "http://localhost:8081");
        private static final String TOPIC = System.getProperty(
                        "kafka.topic", "avro-consume-topic");

        // Simple Avro schema: User { name: string, age: int }
        private static final Schema USER_SCHEMA = new Schema.Parser().parse("{"
                        + "\"type\":\"record\","
                        + "\"name\":\"User\","
                        + "\"namespace\":\"pl.perfluencer.kafka.avro\","
                        + "\"fields\":["
                        + "  {\"name\":\"name\",\"type\":\"string\"},"
                        + "  {\"name\":\"age\",\"type\":\"int\"}"
                        + "]}");

        // ==================== Protocol ====================

        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                        .bootstrapServers(BOOTSTRAP_SERVERS)
                        .groupId("gatling-avro-consume-" + UUID.randomUUID().toString().substring(0, 8))
                        .numProducers(1)
                        .numConsumers(1)
                        .pollTimeout(Duration.ofMillis(100))
                        .producerProperties(Map.of(
                                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                        KafkaAvroSerializer.class.getName(),
                                        "schema.registry.url", SCHEMA_REGISTRY_URL))
                        .consumerProperties(Map.of(
                                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                        KafkaAvroDeserializer.class.getName(),
                                        "schema.registry.url", SCHEMA_REGISTRY_URL,
                                        "auto.offset.reset", "earliest"));

        // ==================== Scenarios ====================

        // Send Avro GenericRecords using the fluent fire-and-forget DSL
        ScenarioBuilder sendScenario = scenario("Send Avro Messages")
                        .exec(
                                        KafkaDsl.kafka("Send Avro User")
                                                        .send()
                                                        .topic(TOPIC)
                                                        .key(session -> UUID.randomUUID().toString())
                                                        .value(session -> {
                                                                GenericRecord user = new GenericData.Record(
                                                                                USER_SCHEMA);
                                                                user.put("name", "User-" + UUID.randomUUID().toString()
                                                                                .substring(0, 8));
                                                                user.put("age", (int) (Math.random() * 60) + 18);
                                                                return user;
                                                        })
                                                        .asAvro());

        // Consume with typed GenericRecord checks
        ScenarioBuilder consumeScenario = scenario("Consume Avro Messages")
                        .exec(
                                        KafkaDsl.consume("Consume Avro", TOPIC)
                                                        .asAvro(GenericRecord.class)
                                                        .check(responseNotEmpty())
                                                        .check("Avro User Check", (GenericRecord received) -> {
                                                                if (received == null) {
                                                                        return Optional.of(
                                                                                        "Received Avro record was null");
                                                                }
                                                                String name = received.get("name").toString();
                                                                int age = (int) received.get("age");
                                                                if (!name.startsWith("User-")) {
                                                                        return Optional.of(
                                                                                        "Expected name starting with 'User-', got: "
                                                                                                        + name);
                                                                }
                                                                if (age < 18 || age > 77) {
                                                                        return Optional.of("Age out of range [18,77]: "
                                                                                        + age);
                                                                }
                                                                return Optional.empty();
                                                        }))
                        .pause(5);

        {
                setUp(
                                sendScenario.injectOpen(atOnceUsers(5)),
                                consumeScenario.injectOpen(nothingFor(1), atOnceUsers(1)))
                                .protocols(kafkaProtocol);
        }
}
