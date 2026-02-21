package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.MessageCheck.*;

/**
 * Demonstrates raw Avro message consumption using thread-based consumers
 * WITHOUT Schema Registry.
 *
 * <p>
 * Sends Avro-encoded {@code User} records as raw binary bytes using the
 * fluent {@code kafka("...").send().asBytes()} DSL (no Schema Registry
 * required)
 * and then consumes them in raw mode with response-only checks that decode
 * and validate the Avro payload using manual Avro serialization logic.
 * </p>
 */
public class KafkaAvroNoRegistrySimulation extends Simulation {

    private static final String BOOTSTRAP_SERVERS = System.getProperty(
            "kafka.bootstrap.servers", "192.168.1.143:9094");
    private static final String TOPIC = System.getProperty(
            "kafka.topic", "avro-noregsitry-topic");

    // ==================== User type ====================

    /**
     * Simple Avro-backed User record with Avro serialization/deserialization.
     */
    static final class User {

        private static final Schema SCHEMA = new Schema.Parser().parse("{"
                + "\"type\":\"record\","
                + "\"name\":\"User\","
                + "\"namespace\":\"pl.perfluencer.kafka.avro\","
                + "\"fields\":["
                + "  {\"name\":\"name\",\"type\":\"string\"},"
                + "  {\"name\":\"age\",\"type\":\"int\"}"
                + "]}");

        final String name;
        final int age;

        User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /** Create a random User for test payloads. */
        static User random() {
            return new User(
                    "User-" + UUID.randomUUID().toString().substring(0, 8),
                    (int) (Math.random() * 60) + 18);
        }

        /** Serialize to Avro binary bytes. */
        byte[] toBytes() {
            try {
                GenericRecord record = new GenericData.Record(SCHEMA);
                record.put("name", name);
                record.put("age", age);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(SCHEMA);
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                writer.write(record, encoder);
                encoder.flush();
                return out.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException("Avro serialization failed", e);
            }
        }

        /** Deserialize from Avro binary bytes. */
        static User fromBytes(byte[] data) {
            try {
                DatumReader<GenericRecord> reader = new GenericDatumReader<>(SCHEMA);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                GenericRecord record = reader.read(null, decoder);
                return new User(record.get("name").toString(), (int) record.get("age"));
            } catch (Exception e) {
                throw new RuntimeException("Avro deserialization failed", e);
            }
        }

        @Override
        public String toString() {
            return "User{name='" + name + "', age=" + age + "}";
        }
    }

    // ==================== Protocol ====================

    KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
            .bootstrapServers(BOOTSTRAP_SERVERS)
            .groupId("gatling-avro-noregsitry-" + UUID.randomUUID().toString().substring(0, 8))
            .numProducers(3)
            .numConsumers(3)
            .pollTimeout(Duration.ofMillis(100))
            .consumerProperties(Map.of("auto.offset.reset", "earliest"));

    // ==================== Scenarios ====================

    ScenarioBuilder sendScenario = scenario("Send Avro Messages")
            .exec(
                    KafkaDsl.kafka("Send Avro User")
                            .send()
                            .topic(TOPIC)
                            .key(session -> UUID.randomUUID().toString())
                            .value(session -> User.random().toBytes())
                            .asBytes());

    ScenarioBuilder consumeScenario = scenario("Consume Avro Messages")
            .exec(
                    KafkaDsl.consume("Consume Avro", TOPIC)
                            .asBytes()
                            .check(responseNotEmpty())
                            .check("Avro User Check", (byte[] receivedBytes) -> {
                                if (receivedBytes == null || receivedBytes.length == 0) {
                                    return Optional.of("Received empty payload");
                                }
                                User user = User.fromBytes(receivedBytes);
                                if (!user.name.startsWith("User-")) {
                                    return Optional.of("Expected name starting with 'User-', got: " + user.name);
                                }
                                if (user.age < 18 || user.age > 77) {
                                    return Optional.of("Age out of range [18,77]: " + user.age);
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
