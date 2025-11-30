package io.github.kbdering.kafka.integration;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
import static org.junit.Assert.assertNotNull;

public class AvroIntegrationTest {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0");
    private static final DockerImageName SCHEMA_REGISTRY_IMAGE = DockerImageName
            .parse("confluentinc/cp-schema-registry:7.4.0");

    private static Network network;
    private static KafkaContainer kafka;
    private static GenericContainer<?> schemaRegistry;

    private static final String TOPIC = "avro-test-topic";
    private static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"User\","
            + "\"fields\":["
            + "  {\"name\":\"name\",\"type\":\"string\"},"
            + "  {\"name\":\"age\",\"type\":\"int\"}"
            + "]}";

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
    public void testAvroProducerConsumer() {
        String schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
        String bootstrapServers = kafka.getBootstrapServers();

        // 1. Create Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        producerProps.put("schema.registry.url", schemaRegistryUrl);

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerProps);

        // 2. Create Avro Record
        Schema schema = new Schema.Parser().parse(USER_SCHEMA);
        GenericRecord user = new GenericData.Record(schema);
        user.put("name", "Alice");
        user.put("age", 30);

        // 3. Send Message
        String key = UUID.randomUUID().toString();
        producer.send(new ProducerRecord<>(TOPIC, key, user));
        producer.flush();
        producer.close();

        // 4. Create Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProps.put("schema.registry.url", schemaRegistryUrl);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // 5. Consume and Verify
        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(10));

        // Retry loop if needed (Testcontainers can be slow initially)
        int retries = 0;
        while (records.isEmpty() && retries < 5) {
            records = consumer.poll(Duration.ofSeconds(2));
            retries++;
        }

        assertEquals("Should receive 1 message", 1, records.count());
        ConsumerRecord<String, GenericRecord> record = records.iterator().next();

        assertEquals("Key should match", key, record.key());
        assertEquals("Name should match", "Alice", record.value().get("name").toString());
        assertEquals("Age should match", 30, record.value().get("age"));

        consumer.close();
    }
}
