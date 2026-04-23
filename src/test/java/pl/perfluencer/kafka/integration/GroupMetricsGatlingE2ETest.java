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

package pl.perfluencer.kafka.integration;

import io.gatling.app.Gatling;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * End-to-end Gatling simulation test using an embedded KafkaContainer.
 *
 * - Starts a real Kafka broker via Testcontainers
 * - Runs an "echo" responder that mirrors request → response topic
 * - Executes the GroupMetricsGatlingSimulation full Gatling run
 * - Produces a Gatling HTML report in target/gatling/
 *
 * Validates that ACK ("send") and E2E metrics are in the correct groups
 * (ungrouped ACK, session-grouped E2E) without any logGroupEnd crashes.
 */
public class GroupMetricsGatlingE2ETest {

    private static final String REQUEST_TOPIC  = "grp-test-requests";
    private static final String RESPONSE_TOPIC = "grp-test-responses";
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0");

    static KafkaContainer kafka;
    static Thread echoThread;
    static AtomicBoolean echoRunning = new AtomicBoolean(false);

    static {
        TestConfig.init();
    }

    @BeforeClass
    public static void startKafka() throws ExecutionException, InterruptedException {
        kafka = new KafkaContainer(KAFKA_IMAGE);
        kafka.start();

        // expose bootstrap servers to the simulation via system properties
        System.setProperty("kafka.bootstrap.servers.e2e", kafka.getBootstrapServers());

        createTopics();
        startEchoResponder();
    }

    @AfterClass
    public static void stopKafka() {
        echoRunning.set(false);
        if (echoThread != null) echoThread.interrupt();
        kafka.stop();
    }

    @Test
    public void runSimulationAndGenerateReport() {
        String[] args = new String[] {
                "-s", GroupMetricsGatlingSimulation.class.getName(),
                "-rf", "target/gatling",
                "-bdf", "target/test-classes"
        };
        Gatling.main(args);
    }

    // ─── Helpers ───────────────────────────────────────────────────────────────

    private static void createTopics() throws ExecutionException, InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(
                    new NewTopic(REQUEST_TOPIC, 1, (short) 1),
                    new NewTopic(RESPONSE_TOPIC, 1, (short) 1)
            )).all().get();
        }
    }

    /**
     * Simple echo responder: reads from REQUEST_TOPIC, writes the same
     * key + value + correlationId header back to RESPONSE_TOPIC.
     */
    private static void startEchoResponder() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "echo-responder-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");

        echoRunning.set(true);
        echoThread = new Thread(() -> {
            try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
                 KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {

                consumer.subscribe(Collections.singletonList(REQUEST_TOPIC));

                while (echoRunning.get() && !Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                    if (!records.isEmpty()) {
                        Thread.sleep(1000); // Wait 1s before responding to the batch
                        for (ConsumerRecord<String, byte[]> record : records) {
                            // Mirror correlationId header to response
                            org.apache.kafka.common.header.Header corrHeader = record.headers().lastHeader("correlationId");
                            ProducerRecord<String, byte[]> response = new ProducerRecord<>(
                                    RESPONSE_TOPIC, record.key(), record.value());
                            if (corrHeader != null) {
                                response.headers().add("correlationId", corrHeader.value());
                            }
                            producer.send(response);
                        }
                    }
                }
            } catch (Exception e) {
                if (echoRunning.get()) {
                    System.err.println("Echo responder error: " + e.getMessage());
                }
            }
        }, "EchoResponder");
        echoThread.setDaemon(true);
        echoThread.start();
    }
}
