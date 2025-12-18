package pl.perfluencer.kafka.integration;

import pl.perfluencer.kafka.actors.KafkaProducerActor;
import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.kafka.util.SerializationType;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Integration test demonstrating feeder-style data-driven testing with Kafka.
 * 
 * This test validates the pattern used in KafkaFeederSimulation by:
 * 1. Loading test data from a CSV resource file
 * 2. Sending each record as a Kafka message
 * 3. Verifying message delivery and content
 */
public class KafkaFeederIntegrationTest {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0");
    private static KafkaContainer kafka;
    private static ActorSystem system;

    @BeforeClass
    public static void setUp() throws ExecutionException, InterruptedException {
        kafka = new KafkaContainer(KAFKA_IMAGE);
        kafka.start();

        createTopic("payment-requests");
        createTopic("payment-responses");

        system = ActorSystem.create("KafkaFeederTestSystem");
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system,
                scala.concurrent.duration.Duration.create(10, java.util.concurrent.TimeUnit.SECONDS), true);
        kafka.stop();
    }

    private static void createTopic(String topicName) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topicName, 4, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }
    }

    /**
     * Test that CSV feeder data can be loaded and used for Kafka message
     * production.
     */
    @Test
    public void testCsvFeederDataLoading() throws IOException {
        List<Map<String, String>> feederData = loadCsvFeeder("payment_data.csv");

        assertFalse("Feeder data should not be empty", feederData.isEmpty());
        assertEquals("Should have 10 payment records", 10, feederData.size());

        // Verify first record structure
        Map<String, String> firstRecord = feederData.get(0);
        assertTrue("Record should have accountId", firstRecord.containsKey("accountId"));
        assertTrue("Record should have amount", firstRecord.containsKey("amount"));
        assertTrue("Record should have currency", firstRecord.containsKey("currency"));

        assertEquals("ACC-001", firstRecord.get("accountId"));
        assertEquals("1500.00", firstRecord.get("amount"));
        assertEquals("USD", firstRecord.get("currency"));
    }

    /**
     * Test sending feeder-driven messages through Kafka producer actor.
     */
    @Test
    public void testFeederDrivenKafkaProduction() throws IOException {
        TestKit probe = new TestKit(system);

        // Load CSV feeder data
        List<Map<String, String>> feederData = loadCsvFeeder("payment_data.csv");

        // Setup Producer Actor
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        ActorRef producerActor = system.actorOf(KafkaProducerActor.props(producerProps, null, null));

        // Send messages using feeder data
        int messageCount = 0;
        for (Map<String, String> record : feederData) {
            String accountId = record.get("accountId");
            String payload = String.format(
                    "{\"accountId\":\"%s\",\"amount\":%s,\"currency\":\"%s\",\"txnId\":\"%s\"}",
                    accountId,
                    record.get("amount"),
                    record.get("currency"),
                    UUID.randomUUID().toString());

            String correlationId = UUID.randomUUID().toString();
            KafkaProducerActor.ProduceMessage msg = new KafkaProducerActor.ProduceMessage(
                    "payment-requests",
                    accountId, // Key from feeder
                    payload,
                    correlationId,
                    true, // waitForAck
                    null,
                    null,
                    scala.collection.immutable.List$.MODULE$.empty());

            producerActor.tell(msg, probe.getRef());
            messageCount++;
        }

        // Verify all messages were acknowledged
        for (int i = 0; i < messageCount; i++) {
            Object ack = probe.receiveOne(Duration.ofSeconds(10));
            assertNotNull("Should receive acknowledgement for message " + i, ack);
        }

        assertEquals("All feeder records should be sent", feederData.size(), messageCount);
    }

    /**
     * Test request store integration with feeder data.
     */
    @Test
    public void testFeederWithRequestStore() throws IOException {
        RequestStore requestStore = new InMemoryRequestStore();
        List<Map<String, String>> feederData = loadCsvFeeder("payment_data.csv");

        // Store requests using feeder data (simulating what the simulation does)
        List<String> correlationIds = new ArrayList<>();
        for (Map<String, String> record : feederData) {
            String correlationId = UUID.randomUUID().toString();
            String payload = String.format(
                    "{\"accountId\":\"%s\",\"amount\":%s}",
                    record.get("accountId"),
                    record.get("amount"));

            requestStore.storeRequest(
                    correlationId,
                    record.get("accountId"), // Key from feeder
                    payload.getBytes(StandardCharsets.UTF_8),
                    SerializationType.STRING,
                    "PaymentTransaction",
                    "FeederTestScenario",
                    System.currentTimeMillis(),
                    30000 // 30s timeout
            );

            correlationIds.add(correlationId);
        }

        // Verify all requests are stored
        for (String correlationId : correlationIds) {
            Map<String, Object> storedRequest = requestStore.getRequest(correlationId);
            assertNotNull("Request should be found: " + correlationId, storedRequest);
            assertNotNull("Request should have key", storedRequest.get(RequestStore.KEY));
        }

        // Simulate response matching (delete request simulating successful response)
        for (String correlationId : correlationIds) {
            requestStore.deleteRequest(correlationId);
            Map<String, Object> deleted = requestStore.getRequest(correlationId);
            assertNull("Request should be deleted after matching", deleted);
        }
    }

    /**
     * Test end-to-end flow: produce from feeder, consume, and verify content.
     */
    @Test
    public void testEndToEndFeederFlow() throws IOException {
        TestKit probe = new TestKit(system);
        List<Map<String, String>> feederData = loadCsvFeeder("payment_data.csv");

        // Take first 3 records for quick test
        List<Map<String, String>> testRecords = feederData.subList(0, 3);
        List<String> sentPayloads = new ArrayList<>();

        // Setup Producer
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ActorRef producerActor = system.actorOf(KafkaProducerActor.props(producerProps, null, null));

        // Send messages
        for (Map<String, String> record : testRecords) {
            String payload = String.format(
                    "{\"accountId\":\"%s\",\"amount\":%s,\"currency\":\"%s\"}",
                    record.get("accountId"),
                    record.get("amount"),
                    record.get("currency"));
            sentPayloads.add(payload);

            KafkaProducerActor.ProduceMessage msg = new KafkaProducerActor.ProduceMessage(
                    "payment-requests",
                    record.get("accountId"),
                    payload,
                    UUID.randomUUID().toString(),
                    true,
                    null,
                    null,
                    scala.collection.immutable.List$.MODULE$.empty());
            producerActor.tell(msg, probe.getRef());
        }

        // Wait for acks
        for (int i = 0; i < testRecords.size(); i++) {
            probe.receiveOne(Duration.ofSeconds(5));
        }

        // Consume and verify
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "feeder-test-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("payment-requests"));

            Set<String> receivedPayloads = new HashSet<>();
            long timeout = System.currentTimeMillis() + 10000; // 10s timeout

            while (receivedPayloads.size() < testRecords.size() && System.currentTimeMillis() < timeout) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    receivedPayloads.add(record.value());
                }
            }

            // Verify all sent payloads were received
            for (String expectedPayload : sentPayloads) {
                assertTrue("Should receive payload: " + expectedPayload,
                        receivedPayloads.contains(expectedPayload));
            }
        }
    }

    /**
     * Helper method to load CSV feeder data from resources.
     * Mimics Gatling's csv() feeder behavior.
     */
    private List<Map<String, String>> loadCsvFeeder(String resourceName) throws IOException {
        List<Map<String, String>> records = new ArrayList<>();

        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourceName);
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                return records;
            }

            String[] headers = headerLine.split(",");

            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                Map<String, String> record = new HashMap<>();
                for (int i = 0; i < headers.length && i < values.length; i++) {
                    record.put(headers[i].trim(), values[i].trim());
                }
                records.add(record);
            }
        }

        return records;
    }
}
