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
import java.util.concurrent.TimeUnit;

import pl.perfluencer.cache.InMemoryRequestStore;

import static io.gatling.javaapi.core.CoreDsl.*;
// Named imports to avoid ambiguity with KafkaDsl.jsonPath() (correlation extractor)
import static pl.perfluencer.common.checks.Checks.responseContains;
import static pl.perfluencer.common.checks.Checks.responseNotEmpty;
import pl.perfluencer.common.checks.Checks; // Checks.jsonPath() to avoid clash with KafkaDsl.jsonPath()

/**
 * Kafka simulation demonstrating the Gatling-style fluent check chain API.
 * 
 * <p>
 * The chain follows: <b>extractor → find strategy → [transform] → validator</b>
 * 
 * <p>
 * The check chain result ({@code MessageCheckResult}) is accepted directly
 * by {@code .check()} on the Kafka request-reply builder — no wrapping needed.
 * 
 * <pre>{@code
 * kafka("Order")
 *                 .requestReply()
 *                 .requestTopic("orders.request")
 *                 .responseTopic("orders.response")
 *                 .asString()
 *                 .send(payload)
 *                 .check(Checks.jsonPath("$.status").find().is("OK"))
 *                 .check(Checks.regex("txnId=([\\w-]+)").find(1).exists())
 * }</pre>
 */
public class KafkaFluentCheckChainSimulation extends Simulation {
        private static final String BOOTSTRAP_SERVERS = System.getProperty(
                        "kafka.bootstrap.servers", "192.168.1.143:9094");
        private static final String TOPIC = System.getProperty(
                        "kafka.topic", "lab-test-topic");

        {
                // 1. Configure the InMemory Request Store for correlation
                // Using in-memory store for simplicity in this example
                InMemoryRequestStore requestStore = new InMemoryRequestStore();

                // 2. Configure the Protocol
                KafkaProtocolBuilder protocol = KafkaDsl.kafka()
                                .bootstrapServers(BOOTSTRAP_SERVERS)
                                .groupId("fluent-checks-group-" + UUID.randomUUID().toString().substring(0, 8))
                                .numProducers(1)
                                .numConsumers(1)
                                .requestStore(requestStore)
                                .metricInjectionInterval(Duration.ofSeconds(1)) // Inject JMX metrics
                                .correlationByKey()
                                .producerProperties(Map.of(
                                                org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                "org.apache.kafka.common.serialization.StringSerializer"))
                                .consumerProperties(Map.of(
                                                org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                                "org.apache.kafka.common.serialization.StringDeserializer",
                                                org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                "latest"));

                // ========================================================
                // Scenario 1: JSON response — fluent JSONPath checks
                // ========================================================
                ScenarioBuilder jsonScenario = scenario("JSON Kafka Checks")
                                .exec(
                                                KafkaDsl.kafka("Create Order")
                                                                .requestReply()
                                                                .requestTopic(TOPIC)
                                                                .responseTopic(TOPIC)
                                                                .asString()
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .send("{\"status\":\"OK\",\"processedCount\":5,\"chargedAmount\":150.50,\"transactionId\":\"TXN-123\",\"items\":[1,2,3],\"region\":\"EU\"}")

                                                                // JSONPath fluent checks
                                                                .check(Checks.jsonPath("$.status").find().is("OK"))
                                                                .check(Checks.jsonPath("$.processedCount").ofInt()
                                                                                .gt(0))
                                                                .check(Checks.jsonPath("$.chargedAmount").ofDouble()
                                                                                .gte(100.0))
                                                                .check(Checks.jsonPath("$.transactionId").find()
                                                                                .exists())
                                                                .check(Checks.jsonPath("$.items[*]").count().gt(0))
                                                                .check(Checks.jsonPath("$.region").find().in("US", "EU",
                                                                                "APAC"))

                                                                .timeout(10, TimeUnit.SECONDS))
                                .pause(Duration.ofSeconds(30));

                // ========================================================
                // Scenario 2: Regex extraction on string responses
                // ========================================================
                ScenarioBuilder regexScenario = scenario("Regex Kafka Checks")
                                .exec(
                                                KafkaDsl.kafka("Query Status")
                                                                .requestReply()
                                                                .requestTopic(TOPIC)
                                                                .responseTopic(TOPIC)
                                                                .asString()
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .send("status=ACTIVE balance=150.75 code=200")

                                                                // Regex fluent checks
                                                                .check(Checks.regex("status=(\\w+)").find(1)
                                                                                .is("ACTIVE"))
                                                                .check(Checks.regex("balance=(\\d+\\.\\d+)")
                                                                                .find(1)
                                                                                .transform(Double::parseDouble)
                                                                                .gt(0.0))
                                                                .check(Checks.regex("code=(\\d{3})")
                                                                                .named("HTTP Status Code")
                                                                                .find(1)
                                                                                .in("200", "201", "202"))

                                                                .timeout(10, TimeUnit.SECONDS))
                                .pause(Duration.ofSeconds(30));

                // ========================================================
                // Scenario 3: XML/SOAP over Kafka — XPath checks
                // ========================================================
                ScenarioBuilder xmlScenario = scenario("XPath Kafka Checks")
                                .exec(
                                                KafkaDsl.kafka("SOAP over Kafka")
                                                                .requestReply()
                                                                .requestTopic(TOPIC)
                                                                .responseTopic(TOPIC)
                                                                .asString()
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .send("<response><status>SUCCESS</status><data><userId>99</userId><items><item>A</item><item>B</item><item>C</item></items></data></response>")

                                                                // XPath fluent checks
                                                                .check(Checks.xpath("/response/status").find()
                                                                                .is("SUCCESS"))
                                                                .check(Checks.xpath("/response/data/userId").find()
                                                                                .not("0"))
                                                                .check(Checks.xpath("/response/data/items/item").count()
                                                                                .is(3))

                                                                .timeout(10, TimeUnit.SECONDS))
                                .pause(Duration.ofSeconds(30));

                // ========================================================
                // Scenario 4: Raw Body checks
                // ========================================================
                ScenarioBuilder bodyScenario = scenario("Body Kafka Checks")
                                .exec(
                                                KafkaDsl.kafka("Health Ping")
                                                                .requestReply()
                                                                .requestTopic(TOPIC)
                                                                .responseTopic(TOPIC)
                                                                .asString()
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .send("PONG SYSTEM_OK")

                                                                // Body string / generic checks
                                                                .check(responseNotEmpty())
                                                                .check(responseContains("PONG"))
                                                                .check(Checks.substring("SYSTEM_OK").find().exists())

                                                                .timeout(10, TimeUnit.SECONDS))
                                .pause(Duration.ofSeconds(30));

                // ========================================================
                // Scenario 5: Mixed Request
                // ========================================================
                ScenarioBuilder mixedScenario = scenario("Kafka Metrics")
                                .exec(
                                                KafkaDsl.kafka("Mixed Request")
                                                                .requestReply()
                                                                .requestTopic(TOPIC)
                                                                .responseTopic(TOPIC)
                                                                .asString()
                                                                .key(session -> UUID.randomUUID().toString())
                                                                .send("{\"type\":\"mixed\",\"payload\":\"data\"}")

                                                                .check(responseNotEmpty())
                                                                .check(Checks.jsonPath("$.type").find().exists())
                                                                .check(Checks.regex("payload\":\"(\\w+)").find(1)
                                                                                .is("data"))

                                                                .timeout(10, TimeUnit.SECONDS))
                                .pause(Duration.ofSeconds(5));

                setUp(
                                jsonScenario.injectOpen(nothingFor(5), atOnceUsers(1)),
                                regexScenario.injectOpen(nothingFor(5), atOnceUsers(1)),
                                xmlScenario.injectOpen(nothingFor(5), atOnceUsers(1)),
                                bodyScenario.injectOpen(nothingFor(5), atOnceUsers(1)),
                                mixedScenario.injectOpen(nothingFor(5), atOnceUsers(1)))
                                .protocols(protocol)
                                .maxDuration(Duration.ofSeconds(30))
                                .assertions(
                                                details("kafka-consumer-lag-max").responseTime().max().lt(100));
        }
}
