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

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.gatling.app.Gatling;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

public class AvroFeederIntegrationTest {

    private static final String TOPIC = "avro-feeder-topic";
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
    public void testAvroWithFeeder() {
        // 1. Run Gatling Simulation
        String[] args = new String[] {
                "-s", AvroFeederSimulation.class.getName(),
                "-rf", "target/gatling",
                "-bdf", "target/test-classes"
        };
        Gatling.main(args);

        // 2. Verify messages in Kafka
        verifyMessages();
    }

    private void verifyMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-feeder-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", System.getProperty("schema.registry.url.e2e"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(15));
            
            assertTrue("Should have received messages from feeder", records.count() > 0);
            
            // Verify content of first record (e.g., Alice or Bob or Charlie or David or Eve)
            GenericRecord first = records.iterator().next().value();
            System.out.println("Verified Avro record from feeder: name=" + first.get("name") + ", age=" + first.get("age"));
            assertTrue("Age should be one of the feeder values", 
                List.of(30, 25, 40, 22, 35).contains(first.get("age")));
        }
    }
}
