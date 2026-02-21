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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

/**
 * Avro simulation with manual serialization — no Schema Registry required.
 *
 * <p>
 * Serializes GenericRecord to byte[] using Avro's BinaryEncoder and
 * uses the default ByteArraySerializer/ByteArrayDeserializer.
 * </p>
 *
 * <p>
 * Configure via system properties:
 * </p>
 * <ul>
 * <li>{@code kafka.bootstrap.servers} - default: localhost:9092</li>
 * <li>{@code kafka.topic} - default: avro-manual-topic</li>
 * </ul>
 */
public class KafkaAvroManualSimulation extends Simulation {

    private static final String BOOTSTRAP_SERVERS = System.getProperty(
            "kafka.bootstrap.servers", "localhost:9092");
    private static final String TOPIC = System.getProperty(
            "kafka.topic", "avro-manual-topic");

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

    /**
     * Serialize a GenericRecord to byte[] using Avro binary encoding.
     */
    private static byte[] serializeAvro(GenericRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Avro serialization failed", e);
        }
    }

    /**
     * Deserialize byte[] back to GenericRecord using the known schema.
     */
    private static GenericRecord deserializeAvro(byte[] data, Schema schema) {
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException("Avro deserialization failed", e);
        }
    }

    {
        RequestStore store = new InMemoryRequestStore();

        // No schema.registry.url needed — uses default ByteArray serializers
        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers(BOOTSTRAP_SERVERS)
                .groupId("gatling-avro-manual-" + UUID.randomUUID().toString().substring(0, 8))
                .numProducers(1)
                .numConsumers(1)
                .pollTimeout(Duration.ofMillis(100))
                .correlationHeaderName("correlationId")
                .requestStore(store);

        // Check: deserialize both sides manually and compare
        MessageCheck<?, ?> nameCheck = new MessageCheck<>(
                "Avro Name Check (Manual)",
                byte[].class, SerializationType.BYTE_ARRAY,
                byte[].class, SerializationType.BYTE_ARRAY,
                (byte[] sentBytes, byte[] receivedBytes) -> {
                    if (receivedBytes == null) {
                        return Optional.of("Received bytes were null");
                    }
                    GenericRecord sent = deserializeAvro(sentBytes, USER_SCHEMA);
                    GenericRecord received = deserializeAvro(receivedBytes, USER_SCHEMA);
                    String sentName = sent.get("name").toString();
                    String receivedName = received.get("name").toString();
                    if (!sentName.equals(receivedName)) {
                        return Optional.of("Name mismatch: sent='" + sentName
                                + "' received='" + receivedName + "'");
                    }
                    return Optional.empty();
                });

        // Scenario
        ScenarioBuilder avroScenario = scenario("Avro Manual Serialization")
                .exec(
                        KafkaDsl.kafka("Avro Manual Request")
                                .requestReply()
                                .topics(TOPIC, TOPIC)
                                .key(session -> UUID.randomUUID().toString())
                                .value(session -> {
                                    GenericRecord user = new GenericData.Record(USER_SCHEMA);
                                    user.put("name", "User-" + UUID.randomUUID().toString().substring(0, 8));
                                    user.put("age", (int) (Math.random() * 60) + 18);
                                    return serializeAvro(user); // Returns byte[]
                                })
                                .asByteArray() // Default serializer, no registry
                                .check(nameCheck)
                                .timeout(10, TimeUnit.SECONDS));

        setUp(
                avroScenario.injectOpen(
                        nothingFor(Duration.ofSeconds(2)),
                        rampUsersPerSec(1).to(10).during(10),
                        constantUsersPerSec(10).during(30),
                        nothingFor(Duration.ofSeconds(5))))
                .protocols(kafkaProtocol);
    }
}
