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
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaSendSimulation extends Simulation {

        {
                KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                                .bootstrapServers("localhost:9092")
                                .groupId("gatling-consumer-group-send-test")
                                .numProducers(1) // Use 1 producer to verify reuse
                                .producerProperties(Map.of(
                                                ProducerConfig.ACKS_CONFIG, "all"));

                ScenarioBuilder scn = scenario("Kafka Send Simulation")
                                .exec(
                                                KafkaDsl.kafka("Req-Wait")
                                                                .send()
                                                                .topic("request_topic")
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .value(session -> "TestValue-Wait-"
                                                                                + UUID.randomUUID().toString())
                                                                .waitForAck()
                                                                .timeout(30, java.util.concurrent.TimeUnit.SECONDS))
                                .exec(
                                                KafkaDsl.kafka("Req-Forget")
                                                                .send()
                                                                .topic("request_topic")
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .value(session -> "TestValue-Forget-"
                                                                                + UUID.randomUUID().toString())
                                                                .timeout(30, java.util.concurrent.TimeUnit.SECONDS));

                setUp(
                                scn.injectOpen(
                                                constantUsersPerSec(10).during(10)))
                                .protocols(kafkaProtocol);
        }
}
