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
import pl.perfluencer.common.util.SerializationType;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.nio.charset.StandardCharsets;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaJsonPathSimulation extends Simulation {

        {
                KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                                .bootstrapServers("localhost:9092")
                                .groupId("gatling-consumer-group-json")
                                .numProducers(1)
                                .numConsumers(1)
                                .correlationExtractor(KafkaDsl.jsonPath("$.correlationId")) // Use JSON Path extractor
                                .producerProperties(Map.of(
                                                ProducerConfig.ACKS_CONFIG, "all"));

                ScenarioBuilder scn = scenario("Kafka JSON Path Simulation")
                                .exec(
                                                KafkaDsl.kafka("request_topic")
                                                                .requestReply()
                                                                .requestTopic("request_topic")
                                                                .responseTopic("response_topic")
                                                                .key(session -> UUID.randomUUID().toString()) // Key
                                                                                                              // (ignored
                                                                                                              // for
                                                                                                              // correlation)
                                                                .value(session -> ("{\"correlationId\": \""
                                                                                + UUID.randomUUID().toString()
                                                                                + "\", \"data\": \"test\"}")
                                                                                .getBytes(StandardCharsets.UTF_8)) // JSON
                                                                                                                   // Body
                                                                                                                   // as
                                                                                                                   // byte[]
                                                                .serializationType(byte[].class, byte[].class,
                                                                                SerializationType.STRING)
                                                                .timeout(10, TimeUnit.SECONDS));

                setUp(
                                scn.injectOpen(
                                                constantUsersPerSec(1).during(10)))
                                .protocols(kafkaProtocol);
        }
}
