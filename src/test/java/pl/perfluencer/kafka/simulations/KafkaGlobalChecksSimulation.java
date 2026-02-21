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
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.javaapi.KafkaDsl.*; // Use standard DSL entry point
import static pl.perfluencer.kafka.MessageCheck.*;

/**
 * Example simulation demonstrating Global Checks vs Scoped Checks.
 *
 * <p>
 * Global Checks: Defined on the protocol, run for EVERY message.
 * Scoped Checks: Defined on the request, run only for that specific request.
 */
public class KafkaGlobalChecksSimulation extends Simulation {

    {
        // 1. Define Global Checks on the Protocol
        // These checks will apply to ALL requests in this simulation
        KafkaProtocolBuilder protocol = pl.perfluencer.kafka.javaapi.KafkaDsl.kafka()
                .bootstrapServers("localhost:9092")
                .numProducers(4)
                .numConsumers(4)
                // Global Check 1: Ensure response is not empty
                .check(responseNotEmpty())
                // Global Check 2: Custom check ensuring no "SYSTEM_ERROR" in body
                // Concisely using builder result directly!
                .check(pl.perfluencer.common.checks.MessageCheckBuilder.strings()
                        .named("No System Error")
                        .check((req, res) -> {
                            if (res.contains("SYSTEM_ERROR")) {
                                return java.util.Optional.of("Response contains SYSTEM_ERROR");
                            }
                            return java.util.Optional.empty();
                        }));

        // ==================== SCENARIOS ====================

        // Scenario 1: Standard Success
        // Global checks + Scoped check (Status=OK) must pass
        ScenarioBuilder successScenario = scenario("Global + Scoped Success")
                .exec(kafka("Standard Request")
                        .requestReply()
                        .requestTopic("orders-request")
                        .responseTopic("orders-response")
                        .key("key1")
                        .value("{\"action\":\"create\"}")
                        // Scoped Check: Specific validation for this request
                        .check(jsonPathEquals("$.status", "OK")));

        // Scenario 2: Global Check Failure (demonstrating Literate DSL)
        ScenarioBuilder failureScenario = scenario("Global Check Failure")
                .exec(kafka("Risky Request")
                        .requestReply()
                        // "Literate" aliases for better readability:
                        .to("orders-request")
                        .replyFrom("orders-response")
                        .correlateBy("key2")
                        .payload("{\"action\":\"delete\"}")
                        .validate(responseNotEmpty()));

        setUp(
                successScenario.injectOpen(atOnceUsers(1)),
                failureScenario.injectOpen(atOnceUsers(1))).protocols(protocol);
    }
}
