package io.github.kbdering.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.github.kbdering.kafka.javaapi.KafkaDsl;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;

import java.time.Duration;

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
                        java.lang.reflect.Method method = kafkaProtocol.getClass().getMethod("metricInjectionInterval",
                                        Duration.class);
                        method.invoke(kafkaProtocol, Duration.ofSeconds(1));

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
