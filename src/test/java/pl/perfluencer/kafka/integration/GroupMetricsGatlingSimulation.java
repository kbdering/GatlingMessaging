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

package pl.perfluencer.kafka.integration;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.common.util.SerializationType;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

/**
 * Gatling simulation used by GroupMetricsGatlingE2ETest.
 *
 * Topology:
 *   - grp-test-requests  → echo responder → grp-test-responses
 *
 * The scenario intentionally wraps the kafka action in two different
 * .group() blocks to validate that:
 *   - "Echo Request send"  is ALWAYS a global, ungrouped row
 *   - "Echo Request"       appears nested under "Order Flow"
 *   - No NoSuchElementException from fake logGroupEnd
 */
public class GroupMetricsGatlingSimulation extends Simulation {

    {
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers.e2e", "localhost:9092");

        KafkaProtocolBuilder protocol = KafkaDsl.kafka()
                .bootstrapServers(bootstrapServers)
                .groupId("grp-metrics-gatling-test")
                .numProducers(1)
                .numConsumers(1)
                .correlationHeaderName("correlationId")
                .awaitConsumersReady(true)
                .seekToEndOnReady(true)
                .consumerReadyTimeout(Duration.ofSeconds(30));

        ScenarioBuilder scn = scenario("Group Metrics Validation Scenario")

                // ── Without any group wrapping ──────────────────────────────────────
                // "Direct Echo send"   → should appear at global level (ungrouped)
                // "Direct Echo"        → should appear at global level too
                .exec(
                        KafkaDsl.kafka("Direct Echo")
                                .requestReply()
                                .requestTopic("grp-test-requests")
                                .responseTopic("grp-test-responses")
                                .key(session -> UUID.randomUUID().toString())
                                .value(session -> ("payload-" + UUID.randomUUID()).getBytes())
                                .serializationType(String.class, String.class, SerializationType.STRING)
                                .timeout(10, TimeUnit.SECONDS)
                )

                // ── Inside a group ──────────────────────────────────────────────────
                // "Order Echo send"    → should appear GLOBALLY (ungrouped, not in "Order Flow")
                // "Order Echo"         → should appear INSIDE the "Order Flow" folder
                .group("Order Flow").on(
                        exec(
                                KafkaDsl.kafka("Order Echo")
                                        .requestReply()
                                        .requestTopic("grp-test-requests")
                                        .responseTopic("grp-test-responses")
                                        .key(session -> UUID.randomUUID().toString())
                                        .value(session -> ("order-" + UUID.randomUUID()).getBytes())
                                        .serializationType(String.class, String.class, SerializationType.STRING)
                                        .timeout(10, TimeUnit.SECONDS)
                        )
                )
                .pause(Duration.ofSeconds(20));

        setUp(
                scn.injectOpen(
                        atOnceUsers(5)
                )
        ).protocols(protocol);
    }
}
