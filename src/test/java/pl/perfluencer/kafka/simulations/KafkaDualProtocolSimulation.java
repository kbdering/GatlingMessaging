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
import pl.perfluencer.common.checks.Checks;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.MessageCheck.*;

/**
 * Demonstrates consuming and producing messages using two distinct Kafka
 * protocols simultaneously in Gatling.
 *
 * <p>
 * This simulation runs two scenarios in parallel:
 * 1. A JSON scenario using String serializers.
 * 2. An Avro scenario using Confluent Schema Registry serializers targeting
 * {@code lab-avro-test-topic}.
 * </p>
 */
public class KafkaDualProtocolSimulation extends Simulation {

        private static final String BOOTSTRAP_SERVERS = System.getProperty(
                        "kafka.bootstrap.servers", "192.168.1.143:9094");

        private static final String JSON_TOPIC = "lab-json-test-topic";
        private static final String AVRO_TOPIC = "lab-avro-test-topic";
        private static final String XML_REQ_TOPIC = "lab-xml-test-topic";

        private static final String SCHEMA_REGISTRY_URL = System.getProperty(
                        "schema.registry.url", "http://localhost:8081");

        // ==================== Schemas & Helpers ====================

        private static final Schema USER_SCHEMA = new Schema.Parser().parse("{"
                        + "\"type\":\"record\","
                        + "\"name\":\"User\","
                        + "\"namespace\":\"pl.perfluencer.kafka.avro\","
                        + "\"fields\":["
                        + "  {\"name\":\"name\",\"type\":\"string\"},"
                        + "  {\"name\":\"age\",\"type\":\"int\"}"
                        + "]}");

        // ==================== Protocols ====================

        // Protocol 1: Standard JSON (String)
        KafkaProtocolBuilder jsonProtocol = KafkaDsl.kafka()
                        .bootstrapServers(BOOTSTRAP_SERVERS)
                        .groupId("gatling-json-" + UUID.randomUUID().toString().substring(0, 8))
                        .numProducers(1)
                        .numConsumers(1)
                        .pollTimeout(Duration.ofMillis(100))
                        .producerProperties(Map.of(
                                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))
                        .consumerProperties(Map.of(
                                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                        StringDeserializer.class.getName(),
                                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                        StringDeserializer.class.getName(),
                                        "auto.offset.reset", "latest"));

        KafkaProtocolBuilder xmlProtocol = KafkaDsl.kafka()
                        .bootstrapServers(BOOTSTRAP_SERVERS)
                        .groupId("gatling-xml-" + UUID.randomUUID().toString().substring(0, 8))
                        .numProducers(1)
                        .numConsumers(1)
                        .producerProperties(Map.of(
                                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"))
                        .consumerProperties(Map.of(
                                        "group.id", "gatling-xml-" + UUID.randomUUID().toString().substring(0, 8),
                                        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                                        "value.deserializer",
                                        "org.apache.kafka.common.serialization.StringDeserializer",
                                        "auto.offset.reset", "latest"));

        // Protocol 2: Avro via Schema Registry
        KafkaProtocolBuilder avroProtocol = KafkaDsl.kafka()
                        .bootstrapServers(BOOTSTRAP_SERVERS)
                        .groupId("gatling-avro-" + UUID.randomUUID().toString().substring(0, 8))
                        .numProducers(1)
                        .numConsumers(1)
                        .pollTimeout(Duration.ofMillis(100))
                        .producerProperties(Map.of(
                                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                        KafkaAvroSerializer.class.getName(),
                                        "schema.registry.url", SCHEMA_REGISTRY_URL))
                        .consumerProperties(Map.of(
                                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                        StringDeserializer.class.getName(),
                                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                        KafkaAvroDeserializer.class.getName(),
                                        "schema.registry.url", SCHEMA_REGISTRY_URL,
                                        "auto.offset.reset", "latest"));

        // ==================== Scenarios ====================

        ScenarioBuilder jsonScenario = scenario("JSON Messaging Flow")
                        .exec(
                                        KafkaDsl.kafka("Send JSON Message")
                                                        .send()
                                                        .topic(JSON_TOPIC)
                                                        .key(session -> UUID.randomUUID().toString())
                                                        .value(session -> "{\"name\": \"User-"
                                                                        + UUID.randomUUID().toString().substring(0, 8)
                                                                        + "\", \"age\": 30}"))
                        .exec(
                                        KafkaDsl.consume("Consume JSON Messages", JSON_TOPIC)
                                                        .asString()
                                                        .check(responseNotEmpty())
                                                        .check(Checks.jsonPath("$.name").find().exists()))
                        .pause(5);

        ScenarioBuilder avroScenario = scenario("Avro Messaging Flow")
                        .exec(
                                        KafkaDsl.kafka("Send Avro Message")
                                                        .send()
                                                        .topic(AVRO_TOPIC)
                                                        .key(session -> UUID.randomUUID().toString())
                                                        .value(session -> {
                                                                GenericRecord user = new GenericData.Record(
                                                                                USER_SCHEMA);
                                                                user.put("name", "User-" + UUID.randomUUID().toString()
                                                                                .substring(0, 8));
                                                                user.put("age", (int) (Math.random() * 60) + 18);
                                                                return user;
                                                        })
                                                        .asAvro())
                        .exec(
                                        KafkaDsl.consume("Consume Avro Messages", AVRO_TOPIC)
                                                        .asAvro(GenericRecord.class)
                                                        .check(responseNotEmpty())
                                                        .check(Checks.field(GenericRecord.class)
                                                                        .get(rec -> rec.get("name").toString())
                                                                        .satisfies(name -> name.startsWith("User-")))
                                                        .check(Checks.field(GenericRecord.class)
                                                                        .get(rec -> (Integer) rec.get("age"))
                                                                        .gt(17))
                                                        .check(Checks.field(GenericRecord.class)
                                                                        .get(rec -> (Integer) rec.get("age"))
                                                                        .lt(78)))
                        .pause(5);

        ScenarioBuilder xmlScenario = scenario("XML Messaging Flow")
                        .exec(
                                        KafkaDsl.kafka("Request-Reply XML Message")
                                                        .requestReply()
                                                        .asString()
                                                        .requestTopic(XML_REQ_TOPIC)
                                                        .responseTopic(XML_REQ_TOPIC)
                                                        .key(session -> UUID.randomUUID().toString())
                                                        .value(session -> "<order><id>"
                                                                        + UUID.randomUUID().toString().substring(0, 8)
                                                                        + "</id><status>NEW</status></order>")
                                                        .check(responseNotEmpty())
                                                        .check(Checks.xpath("//status").find().is("NEW"))
                                                        .timeout(10, TimeUnit.SECONDS))
                        .pause(5);

        // ==================== Simulation Pipeline ====================

        {
                setUp(
                                // Inject the JSON scenario bounded strictly to the String-serialized protocol
                                jsonScenario.injectOpen(nothingFor(5), atOnceUsers(5)).protocols(jsonProtocol),

                                // Inject the Avro scenario bounded strictly to the Schema Registry serialized
                                // protocol
                                avroScenario.injectOpen(nothingFor(5), atOnceUsers(5)).protocols(avroProtocol),

                                // Inject the XML request-reply scenario
                                xmlScenario.injectOpen(nothingFor(5), atOnceUsers(5)).protocols(xmlProtocol))
                                .maxDuration(Duration.ofSeconds(60));
        }
}
