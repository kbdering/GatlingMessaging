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
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.SessionAwareMessageCheck;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.common.util.SerializationType;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.*;

/**
 * Demonstrates Payload Storage Optimization and Session Variable Persistence.
 * 
 * This simulation specifically aims to test high-throughput scenarios where
 * storage
 * I/O is a bottleneck. By skipping payload storage and using session-aware
 * checks,
 * we minimize the write load on the RequestStore (Redis/Postgres).
 */
public class KafkaOptimizationSimulation extends Simulation {

        {
                // 1. Protocol Configuration
                KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                                .bootstrapServers("localhost:9092")
                                .groupId("optimization-group")
                                .numProducers(1)
                                .numConsumers(1)
                                .producerProperties(Map.of(
                                                ProducerConfig.ACKS_CONFIG, "1", // Faster acks for throughput
                                                ProducerConfig.LINGER_MS_CONFIG, "5"));

                // 2. Define Session-Aware Check
                // We only access session variables, as the payload is NOT retrieved from store.
                SessionAwareMessageCheck<String> amountCheck = new SessionAwareMessageCheck<>(
                                "Amount Validation",
                                String.class, SerializationType.STRING,
                                (sessionVars, response) -> {
                                        String expectedAmount = sessionVars.get("amount");
                                        if (expectedAmount == null) {
                                                // Should not happen if storeSession works
                                                return Optional.of("Session variable 'amount' was missing!");
                                        }
                                        if (response.contains(expectedAmount)) {
                                                return Optional.empty();
                                        }
                                        return Optional.of("Response did not contain amount: " + expectedAmount);
                                });

                // 3. Feeder for dynamic data
                // Generates random amounts to verify per-request
                java.util.Iterator<java.util.Map<String, Object>> feeder = Stream.generate(() -> {
                        java.util.Map<String, Object> map = new java.util.HashMap<>();
                        map.put("amount", String.valueOf((int) (Math.random() * 1000)));
                        map.put("reqId", UUID.randomUUID().toString());
                        return map;
                }).iterator();

                // 4. Scenario Definition
                List<MessageCheck<?, ?>> checks = Collections.singletonList(amountCheck);

                ScenarioBuilder scn = scenario("Optimized Kafka Request-Reply")
                                .feed(feeder)
                                .exec(
                                                KafkaDsl.kafka("OptimizedReq")
                                                                .requestReply()
                                                                .requestTopic("request_topic")
                                                                .responseTopic("response_topic")
                                                                .key(session -> session.getString("reqId"))
                                                                .value(session -> "{\"id\":\""
                                                                                + session.getString("reqId")
                                                                                + "\", \"amt\":"
                                                                                + session.getString("amount") + "}")
                                                                .serializationType(String.class, String.class,
                                                                                SerializationType.STRING)
                                                                .checks(checks)
                                                                .timeout(5, TimeUnit.SECONDS)
                                                                // CRITICAL: Persist only what we need for validation
                                                                .storeSession("amount")
                                                                // CRITICAL: Do not write the full payload to
                                                                // RequestStore
                                                                .skipPayloadStorage());

                // 5. Load Injection
                setUp(
                                scn.injectOpen(
                                                rampUsersPerSec(10).to(100).during(Duration.ofSeconds(10)),
                                                constantUsersPerSec(100).during(Duration.ofSeconds(20))))
                                .protocols(kafkaProtocol);
        }
}
