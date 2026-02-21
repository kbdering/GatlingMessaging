package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.javaapi.Kafka.kafka;
import static pl.perfluencer.kafka.MessageCheck.*;

/**
 * Example simulation demonstrating the new fluent Kafka DSL.
 * 
 * <p>
 * With static imports, the syntax matches Gatling's HTTP DSL:
 * 
 * <pre>{@code
 * kafka("Order Request")
 *         .requestReply()
 *         .requestTopic("orders-request")
 *         .responseTopic("orders-response")
 *         .key(session -> "key")
 *         .value(session -> "{\"orderId\":\"123\"}")
 *         .check(echoCheck())
 *         .check(jsonPathEquals("$.status", "OK"))
 *         .timeout(10, SECONDS)
 * }</pre>
 */
public class KafkaFluentDslSimulation extends Simulation {

    {
        // Protocol configuration
        KafkaProtocolBuilder protocol = pl.perfluencer.kafka.javaapi.KafkaDsl.kafka()
                .bootstrapServers("localhost:9092")
                .numProducers(4)
                .numConsumers(4);

        // ==================== SCENARIOS ====================

        // Scenario 1: Basic request-reply with fluent .check() chaining
        ScenarioBuilder basicScenario = scenario("Basic Request-Reply")
                .exec(kafka("Echo Request")
                        .requestReply()
                        .requestTopic("echo-request")
                        .responseTopic("echo-response")
                        .key("test-key")
                        .value("{\"message\":\"Hello Kafka\"}")
                        .check(echoCheck())
                        .check(responseNotEmpty())
                        .timeout(10, TimeUnit.SECONDS));

        // Scenario 2: JSON validation with multiple checks
        ScenarioBuilder jsonScenario = scenario("JSON Validation")
                .exec(kafka("Order Request")
                        .requestReply()
                        .requestTopic("orders-request")
                        .responseTopic("orders-response")
                        .key(session -> "order-" + session.userId())
                        .value(session -> "{\"orderId\":\"ORD-" + session.userId() + "\",\"action\":\"CREATE\"}")
                        .check(jsonPathEquals("$.status", "OK"))
                        .check(responseContains("SUCCESS"))
                        .timeout(Duration.ofSeconds(15)));

        // Scenario 3: Custom check with builder pattern
        ScenarioBuilder customScenario = scenario("Custom Validation")
                .exec(kafka("Transaction Request")
                        .requestReply()
                        .requestTopic("transactions-request")
                        .responseTopic("transactions-response")
                        .key("txn-key")
                        .value("{\"amount\":100.00,\"type\":\"CREDIT\"}")
                        .check(pl.perfluencer.common.checks.MessageCheckBuilder.strings()
                                .named("Amount Validation")
                                .check((req, res) -> {
                                    if (res.contains("ERROR")) {
                                        return java.util.Optional.of("Transaction failed");
                                    }
                                    return java.util.Optional.empty();
                                }))
                        .storeSession("userId", "orderId")
                        .timeout(20, TimeUnit.SECONDS));

        // ==================== SETUP ====================

        setUp(
                basicScenario.injectOpen(
                        rampUsersPerSec(1).to(10).during(Duration.ofSeconds(30))),
                jsonScenario.injectOpen(
                        nothingFor(Duration.ofSeconds(10)),
                        rampUsersPerSec(1).to(5).during(Duration.ofSeconds(20))),
                customScenario.injectOpen(
                        nothingFor(Duration.ofSeconds(20)),
                        constantUsersPerSec(2).during(Duration.ofSeconds(30))))
                .protocols(protocol)
                .assertions(
                        global().successfulRequests().percent().gt(95.0));
    }
}
