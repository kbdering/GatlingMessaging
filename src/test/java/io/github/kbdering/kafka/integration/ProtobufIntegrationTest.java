package io.github.kbdering.kafka.integration;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.github.kbdering.kafka.protobuf.Person;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ProtobufIntegrationTest {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0");
    private static final DockerImageName SCHEMA_REGISTRY_IMAGE = DockerImageName
            .parse("confluentinc/cp-schema-registry:7.4.0");

    private static Network network;
    private static KafkaContainer kafka;
    private static GenericContainer<?> schemaRegistry;

    private static final String TOPIC = "protobuf-test-topic";

    @BeforeClass
    public static void setUp() {
        network = Network.newNetwork();

        kafka = new KafkaContainer(KAFKA_IMAGE)
                .withNetwork(network);
        kafka.start();

        schemaRegistry = new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
                .withNetwork(network)
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafka.getNetworkAliases().get(0) + ":9092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
        schemaRegistry.start();
    }

    @AfterClass
    public static void tearDown() {
        schemaRegistry.stop();
        kafka.stop();
        network.close();
    }

    @Test
    public void testProtobufProducerConsumer() {
        String schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        String bootstrapServers = kafka.getBootstrapServers();

        // 1. Create Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName());
        producerProps.put("schema.registry.url", schemaRegistryUrl);

        KafkaProducer<String, Person> producer = new KafkaProducer<>(producerProps);

        // 2. Create Protobuf Object
        Person person = Person.newBuilder()
                .setName("Bob")
                .setId(42)
                .setEmail("bob@example.com")
                .build();

        // 3. Send Message
        String key = UUID.randomUUID().toString();
        producer.send(new ProducerRecord<>(TOPIC, key, person));
        producer.flush();
        producer.close();

        // 4. Create Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf-test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName());
        consumerProps.put("schema.registry.url", schemaRegistryUrl);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("specific.protobuf.value.type", Person.class.getName()); // Important for specific type

        KafkaConsumer<String, Person> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // 5. Consume and Verify
        ConsumerRecords<String, Person> records = consumer.poll(Duration.ofSeconds(10));

        int retries = 0;
        while (records.isEmpty() && retries < 5) {
            records = consumer.poll(Duration.ofSeconds(2));
            retries++;
        }

        assertEquals("Should receive 1 message", 1, records.count());
        ConsumerRecord<String, Person> record = records.iterator().next();

        assertEquals("Key should match", key, record.key());
        assertEquals("Name should match", "Bob", record.value().getName());
        assertEquals("ID should match", 42, record.value().getId());
        assertEquals("Email should match", "bob@example.com", record.value().getEmail());

        consumer.close();
    }
}
