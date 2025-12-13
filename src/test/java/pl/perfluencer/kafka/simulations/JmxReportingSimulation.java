package pl.perfluencer.kafka.simulation;

import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static io.gatling.javaapi.core.CoreDsl.*;

public class JmxReportingSimulation extends Simulation {

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> dummyConsumer;
    private Thread pollingThread;

    public JmxReportingSimulation() {
        // We expect bootstrap servers to be passed via system property from the Runner
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");

        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers(bootstrapServers)
                .groupId("gatling-report-group")
                .metricInjectionInterval(Duration.ofSeconds(1)) // Report lag every 1s
                .producerProperties(Map.of(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

        ScenarioBuilder scn = scenario("Kafka Metrics")
                .exec(
                        // Simple loop to keep the simulation running and generating metrics
                        repeat(10).on(
                                exec(session -> {
                                    System.out.println(">>> DEBUG: Executing Request loop");
                                    return session;
                                })
                                        .exec(KafkaDsl.kafka("test-request", "any-topic", "key", "value"))
                                        .exec(session -> {
                                            System.out.println(">>> DEBUG: Request executed");
                                            return session;
                                        })
                                        .pause(1)));

        setUp(
                scn.injectOpen(atOnceUsers(1))).protocols(kafkaProtocol);
    }

    @Override
    public void before() {
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        Map<String, Object> props = new java.util.HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dummy-jmx-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        dummyConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        dummyConsumer.subscribe(Collections.singletonList("any-topic"));

        // Poll in background to ensure initialization and "liveness" for JMX
        pollingThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    dummyConsumer.poll(Duration.ofMillis(100));
                }
            } catch (Exception e) {
                // ignore
            }
        });
        pollingThread.start();
    }

    @Override
    public void after() {
        if (pollingThread != null) {
            pollingThread.interrupt();
        }
        if (dummyConsumer != null) {
            try {
                dummyConsumer.wakeup();
                dummyConsumer.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
