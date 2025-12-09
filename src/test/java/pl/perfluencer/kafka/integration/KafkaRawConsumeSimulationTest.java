package pl.perfluencer.kafka.integration;

import io.gatling.app.Gatling;
import pl.perfluencer.kafka.simulations.KafkaRawConsumeSimulation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaRawConsumeSimulationTest {

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
    public void testRawConsume() {
        String[] args = new String[] {
                "-s", KafkaRawConsumeSimulation.class.getName(),
                "-rf", "target/gatling",
                "-rd", "raw-consume-test"
        };
        io.gatling.app.Gatling$.MODULE$.fromArgs(args);
    }
}
