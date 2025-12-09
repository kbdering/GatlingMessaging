package pl.perfluencer.kafka.integration;

import io.gatling.app.Gatling;
import pl.perfluencer.kafka.simulations.KafkaRawConsumeSimulation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class KafkaJmxMetricsTest {

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
    public void testJmxMetricsExposed() throws InterruptedException {
        // Start JMX poller in a separate thread
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                // Wait for Gatling to start and Producer to initialize
                Thread.sleep(5000);
                checkJmxMetrics();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        String[] args = new String[] {
                "-s", KafkaRawConsumeSimulation.class.getName(),
                "-rf", "target/gatling",
                "-rd", "jmx-metrics-test"
        };

        // Use fromArgs to avoid System.exit
        io.gatling.app.Gatling$.MODULE$.fromArgs(args);

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
    }

    private void checkJmxMetrics() throws Exception {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        // Check for Producer Metrics
        // Pattern: kafka.producer:type=producer-metrics,client-id=*
        Set<ObjectName> producerMetrics = mBeanServer
                .queryNames(new ObjectName("kafka.producer:type=producer-metrics,client-id=*"), null);
        System.out.println("Found Producer Metrics MBeans: " + producerMetrics.size());
        if (!producerMetrics.isEmpty()) {
            for (ObjectName name : producerMetrics) {
                System.out.println(" - " + name);
                // Check a specific attribute, e.g., record-send-rate
                // Note: Attributes are Double
                try {
                    Object sendRate = mBeanServer.getAttribute(name, "record-send-rate");
                    System.out.println("   record-send-rate: " + sendRate);
                } catch (Exception e) {
                    System.out.println("   Could not read record-send-rate: " + e.getMessage());
                }
            }
        }
        assertTrue("Should find at least one Kafka Producer Metrics MBean", !producerMetrics.isEmpty());

        // Check for Consumer Metrics
        // Pattern: kafka.consumer:type=consumer-metrics,client-id=*
        Set<ObjectName> consumerMetrics = mBeanServer
                .queryNames(new ObjectName("kafka.consumer:type=consumer-metrics,client-id=*"), null);
        System.out.println("Found Consumer Metrics MBeans: " + consumerMetrics.size());
        assertTrue("Should find at least one Kafka Consumer Metrics MBean", !consumerMetrics.isEmpty());
    }
}
