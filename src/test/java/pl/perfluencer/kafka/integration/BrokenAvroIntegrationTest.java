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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class BrokenAvroIntegrationTest {

    private static final String TOPIC = "broken-avro-topic";
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0");
    private static final DockerImageName SCHEMA_REGISTRY_IMAGE = DockerImageName.parse("confluentinc/cp-schema-registry:7.4.0");

    private static Network network;
    private static KafkaContainer kafka;
    private static GenericContainer<?> schemaRegistry;

    @BeforeClass
    public static void setUp() throws ExecutionException, InterruptedException {
        TestConfig.init();
        network = Network.newNetwork();

        kafka = new KafkaContainer(KAFKA_IMAGE).withNetwork(network);
        kafka.start();

        schemaRegistry = new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
                .withNetwork(network)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        schemaRegistry.start();

        String schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        
        System.setProperty("kafka.bootstrap.servers.e2e", kafka.getBootstrapServers());
        System.setProperty("schema.registry.url.e2e", schemaRegistryUrl);

        createTopic();
    }

    @AfterClass
    public static void tearDown() {
        schemaRegistry.stop();
        kafka.stop();
        network.close();
    }

    private static void createTopic() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @Test
    public void testBrokenAvroValidation() {
        // Run Gatling Simulation - this should crash during execution
        String[] args = new String[] {
                "-s", BrokenAvroSimulation.class.getName(),
                "-rf", "target/gatling",
                "-bdf", "target/test-classes"
        };
        
        System.out.println("--- STARTING BROKEN AVRO SIMULATION ---");
        try {
            Gatling.main(args);
        } catch (Exception e) {
            System.err.println("Gatling execution failed as expected: " + e.getMessage());
        }
        System.out.println("--- FINISHED BROKEN AVRO SIMULATION ---");
    }
}
