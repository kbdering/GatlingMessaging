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

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.MessageCheck.*;

/**
 * Showcase simulation demonstrating message assertion capabilities.
 *
 * <p>
 * Demonstrates various check types available in the framework:
 * </p>
 * <ul>
 * <li><b>Echo Check</b> — verifies response exactly matches request</li>
 * <li><b>Response Contains</b> — checks substring presence</li>
 * <li><b>Response Not Empty</b> — validates non-empty response</li>
 * <li><b>JSON Path Equals</b> — validates a specific JSON field value</li>
 * <li><b>JSON Field Equals</b> — cross-validates JSON fields between
 * request/response</li>
 * <li><b>Custom Check</b> — arbitrary validation logic via BiFunction</li>
 * </ul>
 *
 * <p>
 * Uses same-topic request-reply (echo pattern) to demonstrate assertions
 * against the consumed response.
 * </p>
 */
public class KafkaMessageAssertionSimulation extends Simulation {

    private static final String BOOTSTRAP_SERVERS = System.getProperty(
            "kafka.bootstrap.servers", "localhost:9092");
    private static final String TOPIC = System.getProperty(
            "kafka.topic", "assertion-demo-topic");

    {
        RequestStore store = new InMemoryRequestStore();

        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers(BOOTSTRAP_SERVERS)
                .groupId("gatling-assertion-demo-" + UUID.randomUUID().toString().substring(0, 8))
                .numProducers(1)
                .numConsumers(1)
                .pollTimeout(Duration.ofMillis(100))
                .requestStore(store);

        // ==================== CHECK DEFINITIONS ====================

        // 1. Echo Check — response must exactly match the request string
        MessageCheck<String, String> echo = echoCheck();

        // 2. Response Not Empty — basic sanity check
        MessageCheck<String, String> notEmpty = responseNotEmpty();

        // 3. Response Contains — verify substring is present
        MessageCheck<String, String> containsOrderId = responseContains("orderId");

        // 4. JSON Path Equals — verify a specific JSON field has expected value
        MessageCheck<String, String> statusOk = jsonPathEquals("$.status", "pending");

        // 5. JSON Field Equals — cross-check: request field echoed in response
        MessageCheck<String, String> orderIdMatch = jsonFieldEquals("$.orderId", "$.orderId");

        // 6. Custom Check — arbitrary validation logic
        MessageCheck<String, String> customLengthCheck = new MessageCheck<>(
                "Response Length Check",
                String.class, pl.perfluencer.common.util.SerializationType.STRING,
                String.class, pl.perfluencer.common.util.SerializationType.STRING,
                (request, response) -> {
                    if (response == null || response.isEmpty()) {
                        return java.util.Optional.of("Response was empty");
                    }
                    if (response.length() < 10) {
                        return java.util.Optional.of("Response too short: " + response.length() + " chars");
                    }
                    return java.util.Optional.empty(); // Pass
                });

        // ==================== SCENARIOS ====================

        // Scenario 1: Simple echo with basic checks
        ScenarioBuilder echoScenario = scenario("Echo Assertions")
                .exec(
                        KafkaDsl.kafka("Echo Request")
                                .requestReply()
                                .topics(TOPIC, TOPIC)
                                .key(session -> UUID.randomUUID().toString())
                                .value(session -> "Hello from Gatling: " + UUID.randomUUID())
                                .check(echo)
                                .check(notEmpty)
                                .check(customLengthCheck)
                                .timeout(10, TimeUnit.SECONDS));

        // Scenario 2: JSON payload with multiple field assertions
        ScenarioBuilder jsonScenario = scenario("JSON Assertions")
                .exec(
                        KafkaDsl.kafka("JSON Order Request")
                                .requestReply()
                                .topics(TOPIC, TOPIC)
                                .key(session -> UUID.randomUUID().toString())
                                .value(session -> {
                                    String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);
                                    return "{\"orderId\":\"" + orderId + "\","
                                            + "\"status\":\"pending\","
                                            + "\"amount\":" + ((int) (Math.random() * 1000) + 1) + "}";
                                })
                                .check(notEmpty)
                                .check(containsOrderId)
                                .check(statusOk)
                                .check(orderIdMatch)
                                .timeout(10, TimeUnit.SECONDS));

        // ==================== LOAD PROFILE ====================

        setUp(
                echoScenario.injectOpen(
                        nothingFor(Duration.ofSeconds(1)),
                        rampUsersPerSec(1).to(5).during(5),
                        constantUsersPerSec(5).during(15)),
                jsonScenario.injectOpen(
                        nothingFor(Duration.ofSeconds(1)),
                        rampUsersPerSec(1).to(5).during(5),
                        constantUsersPerSec(5).during(15)))
                .protocols(kafkaProtocol);
    }
}
