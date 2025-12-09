package pl.perfluencer.kafka.integration;

import pl.perfluencer.kafka.simulations.KafkaTimeoutVolumeSimulation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaTimeoutVolumeTest {

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
    public void testTimeoutVolume() {
        String[] args = new String[] {
                "-s", KafkaTimeoutVolumeSimulation.class.getName(),
                "-rf", "target/gatling",
                "-rd", "timeout-volume-test"
        };
        io.gatling.app.Gatling$.MODULE$.fromArgs(args);
    }
}
