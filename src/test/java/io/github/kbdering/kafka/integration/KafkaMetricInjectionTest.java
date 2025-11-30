package io.github.kbdering.kafka.integration;

import io.gatling.app.Gatling;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaMetricInjectionTest {

    public static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @BeforeClass
    public static void setup() {
        kafka.start();
        System.setProperty("kafka.bootstrap.servers", kafka.getBootstrapServers());
    }

    @AfterClass
    public static void teardown() {
        kafka.stop();
        System.clearProperty("kafka.bootstrap.servers");
    }

    @Test
    public void testMetricInjection() {
        String[] args = new String[] {
                "-s", io.github.kbdering.kafka.simulations.MetricInjectionSimulation.class.getName(),
                "-rf", "target/gatling",
                "-rd", "metric-injection-test"
        };
        // Use fromArgs to avoid System.exit
        int exitCode = io.gatling.app.Gatling$.MODULE$.fromArgs(args);
        // Assert exitCode if needed, e.g., Assert.assertEquals(0, exitCode);
    }
}
