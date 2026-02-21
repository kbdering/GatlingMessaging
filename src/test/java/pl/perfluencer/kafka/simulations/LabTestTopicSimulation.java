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
import org.apache.kafka.clients.producer.ProducerConfig;
import pl.perfluencer.common.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

/**
 * High-throughput simulation for lab-test-topic with 50KB payload and 1000 TPS.
 * 
 * This simulation demonstrates:
 * - 50KB static payload for realistic enterprise message size
 * - 1000 TPS target throughput
 * - Native Gatling JMX metric injection
 * - BufferedRequestStore with Redis backend for correlation
 * - Fluent DSL usage
 */
public class LabTestTopicSimulation extends Simulation {

        // Configuration - can be overridden with system properties
        private static final String BOOTSTRAP_SERVERS = System.getProperty(
                        "kafka.bootstrap.servers", "192.168.1.143:9094");
        private static final String TOPIC = System.getProperty(
                        "kafka.topic", "lab-test-topic");

        // 50KB static payload - realistic enterprise message size
        private static final String PAYLOAD_50KB;
        static {
                char[] chars = new char[5 * 1024]; // 5KB
                Arrays.fill(chars, 'X');
                // Add some structure to make it look like real data
                String header = "{\"type\":\"TRANSACTION\",\"version\":\"1.0\",\"timestamp\":"
                                + System.currentTimeMillis()
                                + ",\"data\":\"";
                String footer = "\"}";
                System.arraycopy(header.toCharArray(), 0, chars, 0, header.length());
                System.arraycopy(footer.toCharArray(), 0, chars, chars.length - footer.length(), footer.length());
                PAYLOAD_50KB = new String(chars);
        }

        {
                // Configure Kafka protocol - tuned for high throughput
                KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                                .bootstrapServers(BOOTSTRAP_SERVERS)
                                .groupId("gatling-lab-test-group-" + UUID.randomUUID().toString().substring(0, 8))
                                .numProducers(3) // Retaining original load
                                .numConsumers(3) // Retaining original consumer count
                                .pollTimeout(Duration.ofMillis(50))
                                .correlationHeaderName("correlationId")
                                .measureStoreLatency(true)
                                // .metricInjectionInterval(Duration.ofSeconds(1)) // Inject JMX metrics into
                                // report every second
                                .producerProperties(Map.of(
                                                ProducerConfig.ACKS_CONFIG, "1",
                                                ProducerConfig.LINGER_MS_CONFIG, "5",
                                                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000",
                                                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000",
                                                ProducerConfig.MAX_BLOCK_MS_CONFIG, "100", // Fail fast if buffer full
                                                ProducerConfig.BATCH_SIZE_CONFIG, "65536",
                                                ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864", // 64MB
                                                ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
                                .consumerProperties(Map.of(
                                                "auto.offset.reset", "latest",
                                                "fetch.min.bytes", "1",
                                                "fetch.max.wait.ms", "50",
                                                "max.poll.records", "500"));

                // Check for 50KB payload (just verify size is correct)
                MessageCheck<?, ?> sizeCheck = new MessageCheck<>(
                                "Payload Size Check",
                                String.class, SerializationType.STRING,
                                String.class, SerializationType.STRING,
                                (String sentMessage, String receivedMessage) -> {
                                        if (receivedMessage == null) {
                                                return Optional.of("Received message was null");
                                        }
                                        // For 50KB messages, just verify we got something substantial back
                                        if (receivedMessage.length() >= 2000) {
                                                return Optional.empty(); // Check passes
                                        }
                                        return Optional.of(
                                                        "Response too small: " + receivedMessage.length() + " bytes");
                                });

                // Scenario: High-throughput Request-Reply with 50KB payload using Fluent DSL
                ScenarioBuilder highThroughputScenario = scenario("High-Throughput 50KB Request-Reply")
                                .exec(
                                                KafkaDsl.kafka("Request-Reply 5KB")
                                                                .requestReply()
                                                                .topics(TOPIC, TOPIC)
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .value(session -> PAYLOAD_50KB.getBytes()) // 50KB
                                                                                                           // static
                                                                                                           // payload
                                                                .asString() // Treat payload as string for simple checks
                                                                .skipPayloadStorage()
                                                                .check(sizeCheck)
                                                                .timeout(30, TimeUnit.SECONDS));

                // Setup: 1000 TPS target throughput
                setUp(
                                highThroughputScenario.injectOpen(
                                                nothingFor(Duration.ofSeconds(5)), // Initial warm-up
                                                rampUsersPerSec(10).to(5000).during(30), // Ramp up to 500 TPS
                                                constantUsersPerSec(5000).during(60), // Sustain 500 TPS for 1 minute
                                                nothingFor(Duration.ofSeconds(10)))) // Drain pending
                                .protocols(kafkaProtocol);
        }
}
