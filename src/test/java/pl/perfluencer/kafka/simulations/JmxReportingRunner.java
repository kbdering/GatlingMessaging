package pl.perfluencer.kafka.simulation;

import io.gatling.app.Gatling;

import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

public class JmxReportingRunner {

    @Test
    public void runSimulation() {
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))) {
            kafka.start();

            // Pass bootstrap servers to the Simulation via System Property
            System.setProperty("kafka.bootstrap.servers", kafka.getBootstrapServers());

            // Run Gatling programmatically using standard args
            String[] args = new String[] {
                    "-s", "pl.perfluencer.kafka.simulation.JmxReportingSimulation",
                    "-rf", "target/gatling-reports",
                    "-bdf", "target/test-classes" // binaries directory
            };

            Gatling.main(args);
        }
    }
}
