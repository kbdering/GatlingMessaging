package pl.perfluencer.kafka.integration;

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
                "--simulation", pl.perfluencer.kafka.simulations.MetricInjectionSimulation.class.getName(),
                "--results-folder", "target/gatling"
        };
        // Use fromArgs to avoid System.exit
        io.gatling.app.Gatling$.MODULE$.fromArgs(args);
    }
}
