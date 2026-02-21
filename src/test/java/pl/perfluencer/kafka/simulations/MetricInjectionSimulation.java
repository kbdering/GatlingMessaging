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

import static io.gatling.javaapi.core.CoreDsl.*;

public class MetricInjectionSimulation extends Simulation {

        KafkaProtocolBuilder kafkaProtocol;

        ScenarioBuilder scn = scenario("Metric Injection Scenario")
                        .exec(KafkaDsl.kafka("request_topic", "key", "value"))
                        .pause(5); // Run for 5 seconds to allow multiple metric injections

        {
                System.out.println("DEBUG: MetricInjectionSimulation initialized. Bootstrap: "
                                + System.getProperty("kafka.bootstrap.servers"));
                try {
                        kafkaProtocol = KafkaDsl.kafka()
                                        .bootstrapServers(
                                                        System.getProperty("kafka.bootstrap.servers", "localhost:9092"))
                                        .groupId("metric-injection-group");

                        // Use reflection to call metricInjectionInterval to avoid potential linkage
                        // issues during test loading
                        // java.lang.reflect.Method method = kafkaProtocol.getClass().getMethod("metricInjectionInterval",
                        //                 java.time.Duration.class);
                        // method.invoke(kafkaProtocol, java.time.Duration.ofSeconds(1));

                        System.out.println("DEBUG: KafkaProtocolBuilder created successfully.");
                } catch (Throwable e) {
                        System.err.println("DEBUG: Failed to create KafkaProtocolBuilder");
                        e.printStackTrace();
                        throw new RuntimeException(e);
                }

                setUp(
                                scn.injectOpen(atOnceUsers(1))).protocols(kafkaProtocol);
        }
}
