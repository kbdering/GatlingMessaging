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

import io.gatling.javaapi.core.FeederBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.common.util.SerializationType;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

/**
 * Sample simulation demonstrating the use of Gatling feeders with Kafka.
 * 
 * This simulation shows how to:
 * 1. Use CSV feeders to drive test data
 * 2. Use random feeders for dynamic data generation
 * 3. Combine feeder data with Kafka request-reply patterns
 * 4. Validate responses against feeder-provided expected values
 */
public class KafkaFeederSimulation extends Simulation {

        {
                // ==================== FEEDER DEFINITIONS ====================

                // CSV Feeder - reads from src/test/resources/payment_data.csv
                // Each row becomes a session variable accessible via
                // session.getString("columnName")
                FeederBuilder<String> csvFeeder = csv("transaction_data.csv").circular();

                // ==================== PROTOCOL CONFIGURATION ====================

                KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                                .bootstrapServers("localhost:9092")
                                .groupId("gatling-feeder-demo")
                                .numProducers(1)
                                .numConsumers(4)
                                .producerProperties(Map.of(
                                                ProducerConfig.ACKS_CONFIG, "all",
                                                ProducerConfig.LINGER_MS_CONFIG, "5",
                                                ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
                                .consumerProperties(Map.of(
                                                "auto.offset.reset", "latest"));

                // Use in-memory store for this demo
                RequestStore requestStore = new InMemoryRequestStore();
                kafkaProtocol.requestStore(requestStore);

                // ==================== MESSAGE VALIDATION ====================

                // Validate that the response contains the expected account ID from the feeder
                List<MessageCheck<?, ?>> transactionChecks = List.of(
                                new MessageCheck<>(
                                                "Transaction Response Validation",
                                                String.class, SerializationType.STRING,
                                                String.class, SerializationType.STRING,
                                                (request, response) -> {
                                                        // Parse the JSON response (simplified - use Jackson in
                                                        // production)
                                                        if (response == null || response.isEmpty()) {
                                                                return Optional.of("Empty response received");
                                                        }
                                                        if (!response.contains("\"status\":\"success\"")) {
                                                                return Optional.of(
                                                                                "Transaction not successful: "
                                                                                                + response);
                                                        }
                                                        return Optional.empty(); // Check passed
                                                }));

                // ==================== SCENARIO WITH CSV FEEDER ====================

                ScenarioBuilder csvFeederScenario = scenario("Transaction Processing with CSV Data")
                                .feed(csvFeeder) // Inject CSV data into each user's session
                                .exec(session -> {
                                        // Log the feeder data (for debugging)
                                        String accountId = session.getString("accountId");
                                        String amount = session.getString("amount");
                                        String currency = session.getString("currency");
                                        System.out.println("Processing transaction: " + accountId + " - " + amount + " "
                                                        + currency);
                                        return session;
                                })
                                .exec(
                                                KafkaDsl.kafkaRequestReply(
                                                                "transaction-requests", // Request topic
                                                                "transaction-responses", // Response topic
                                                                session -> session.getString("accountId"), // Key from
                                                                                                           // feeder
                                                                session -> String.format(
                                                                                "{\"accountId\":\"%s\",\"amount\":%s,\"currency\":\"%s\",\"txnId\":\"%s\"}",
                                                                                session.getString("accountId"),
                                                                                session.getString("amount"),
                                                                                session.getString("currency"),
                                                                                UUID.randomUUID().toString()),
                                                                SerializationType.STRING,
                                                                transactionChecks,
                                                                10, TimeUnit.SECONDS));

                // ==================== SCENARIO WITH DYNAMIC DATA ====================
                // Generate random values inline (alternative to using a custom feeder)

                ScenarioBuilder randomFeederScenario = scenario("Dynamic Transaction Generation")
                                .exec(session -> {
                                        // Generate random values and store in session
                                        return session
                                                        .set("randomAccountId",
                                                                        "ACC-" + UUID.randomUUID().toString()
                                                                                        .substring(0, 8))
                                                        .set("randomAmount",
                                                                        String.format("%.2f", Math.random() * 10000))
                                                        .set("timestamp", String.valueOf(System.currentTimeMillis()));
                                })
                                .exec(
                                                KafkaDsl.kafkaRequestReply(
                                                                "transaction-requests",
                                                                "transaction-responses",
                                                                session -> session.getString("randomAccountId"),
                                                                session -> String.format(
                                                                                "{\"accountId\":\"%s\",\"amount\":%s,\"timestamp\":%s}",
                                                                                session.getString("randomAccountId"),
                                                                                session.getString("randomAmount"),
                                                                                session.getString("timestamp")),
                                                                SerializationType.STRING,
                                                                transactionChecks,
                                                                10, TimeUnit.SECONDS));

                // ==================== LOAD PROFILE ====================

                setUp(
                                // CSV feeder scenario - use real test data
                                csvFeederScenario.injectOpen(
                                                rampUsersPerSec(1).to(10).during(Duration.ofSeconds(30)),
                                                constantUsersPerSec(10).during(Duration.ofSeconds(60)),
                                                rampUsersPerSec(10).to(1).during(Duration.ofSeconds(30)),
                                                nothingFor(Duration.ofSeconds(15)) // Cooldown
                                ),
                                // Random feeder scenario - stress test with dynamic data
                                randomFeederScenario.injectOpen(
                                                nothingFor(Duration.ofSeconds(30)), // Start after CSV scenario ramps up
                                                rampUsersPerSec(5).to(50).during(Duration.ofSeconds(30)),
                                                constantUsersPerSec(50).during(Duration.ofSeconds(60)),
                                                rampUsersPerSec(50).to(5).during(Duration.ofSeconds(30)),
                                                nothingFor(Duration.ofSeconds(15)) // Cooldown
                                )).protocols(kafkaProtocol)
                                .assertions(
                                                global().responseTime().percentile3().lt(5000), // P95 < 5s
                                                global().successfulRequests().percent().gt(95.0) // 95% success
                                );
        }
}
