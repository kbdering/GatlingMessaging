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

import io.gatling.javaapi.core.*;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaTimeoutVolumeSimulation extends Simulation {

        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String topic = "timeout-volume-topic";

        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                        .bootstrapServers(bootstrapServers)
                        .groupId("timeout-group")
                        .timeoutCheckInterval(Duration.ofMillis(100)) // Check every 100ms
                        .producerProperties(Map.of(
                                        ProducerConfig.ACKS_CONFIG, "1",
                                        ProducerConfig.LINGER_MS_CONFIG, "0"));

        ScenarioBuilder scn = scenario("Timeout Volume Scenario")
                        .exec(
                                        KafkaDsl.kafka("Timeout Request")
                                                        .requestReply()
                                                        .requestTopic(topic)
                                                        .responseTopic("reply-topic")
                                                        .key(session -> "key")
                                                        .value(session -> "value")
                                                        .timeout(500, java.util.concurrent.TimeUnit.MILLISECONDS))
                        .pause(1);

        {
                setUp(
                                scn.injectOpen(
                                                rampUsersPerSec(10).to(100).during(10), // Ramp up to 100 req/s
                                                constantUsersPerSec(100).during(10) // Hold for 10s
                                )).protocols(kafkaProtocol);
        }
}
