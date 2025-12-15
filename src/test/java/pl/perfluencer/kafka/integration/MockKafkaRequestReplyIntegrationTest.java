package pl.perfluencer.kafka.integration;

import io.gatling.commons.stats.Status;
import io.gatling.commons.util.Clock;
import io.gatling.core.stats.StatsEngine;
import pl.perfluencer.kafka.actors.KafkaProducerActor;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import pl.perfluencer.kafka.cache.InMemoryRequestStore;
import pl.perfluencer.kafka.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.MessageProcessor;
import pl.perfluencer.kafka.cache.RequestStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests for request-reply flow using mocked Kafka clients.
 * These tests verify the full end-to-end flow without requiring
 * Docker/Testcontainers.
 */
public class MockKafkaRequestReplyIntegrationTest {

    private ActorSystem system;
    private MockProducer<String, Object> mockProducer;
    private MockConsumer<String, Object> mockConsumer;
    private RequestStore requestStore;
    private StatsEngine stubStatsEngine;
    private TestStatsRecorder statsRecorder;
    private MessageProcessor processor;

    @Before
    public void setUp() {
        system = ActorSystem.create("MockKafkaIntegrationTest");
        @SuppressWarnings("unchecked")
        org.apache.kafka.common.serialization.Serializer<Object> valueSerializer = (org.apache.kafka.common.serialization.Serializer<Object>) (org.apache.kafka.common.serialization.Serializer<?>) new ByteArraySerializer();
        mockProducer = new MockProducer<>(true, new StringSerializer(), valueSerializer);
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        requestStore = new InMemoryRequestStore();
        statsRecorder = new TestStatsRecorder();
        stubStatsEngine = createStubStatsEngine();

        // Create MessageProcessor with required dependencies
        Clock clock = () -> System.currentTimeMillis();
        List<MessageCheck<?, ?>> checks = Collections.emptyList();
        CorrelationExtractor correlationExtractor = new pl.perfluencer.kafka.extractors.HeaderExtractor(
                "correlationId");
        processor = new MessageProcessor(requestStore, stubStatsEngine, clock, ActorRef.noSender(), checks,
                correlationExtractor,
                false);

        // Setup mock consumer with topic partition
        TopicPartition partition = new TopicPartition("response-topic", 0);
        mockConsumer.assign(Collections.singletonList(partition));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));
    }

    @After
    public void tearDown() throws Exception {
        if (requestStore != null) {
            requestStore.close();
        }
        if (system != null) {
            TestKit.shutdownActorSystem(system,
                    scala.concurrent.duration.Duration.create(5, java.util.concurrent.TimeUnit.SECONDS), true);
        }
    }

    @Test
    public void testSuccessfulRequestReplyFlow() throws InterruptedException {
        TestKit probe = new TestKit(system);
        String correlationId = "test-corr-123";
        String requestKey = "request-key";
        byte[] requestValue = "request-value".getBytes();
        String transactionName = "TestRequest";
        String scenarioName = "TestScenario";

        // 1. Create producer actor and send request
        ActorRef producerActor = system.actorOf(KafkaProducerActor.props(mockProducer, null, null));
        producerActor.tell(
                new KafkaProducerActor.ProduceMessage("request-topic", requestKey, requestValue, correlationId, true,
                        null, null, scala.collection.immutable.List$.MODULE$.empty()),
                probe.getRef());

        Thread.sleep(100); // Give actor time to process

        // 2. Store request (simulating what KafkaRequestReplyAction does)
        long startTime = System.currentTimeMillis();
        requestStore.storeRequest(correlationId, requestKey, requestValue, SerializationType.STRING,
                transactionName, scenarioName, startTime, 30000);

        // 3. Verify request was sent to Kafka
        assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, Object> sentRecord = mockProducer.history().get(0);
        assertEquals("request-topic", sentRecord.topic());
        assertEquals(requestKey, sentRecord.key());
        assertArrayEquals(requestValue, (byte[]) sentRecord.value());

        // 4. Simulate response from application
        byte[] responseValue = "response-value".getBytes();
        ConsumerRecord<String, Object> responseRecord = new ConsumerRecord<>(
                "response-topic", 0, 0L,
                requestKey, (Object) responseValue);
        responseRecord.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        // 5. Process response using MessageProcessor
        processor.process(Collections.singletonList(responseRecord));

        // 6. Verify stats were recorded
        assertTrue("Stats should be recorded", statsRecorder.wasResponseLogged());
        assertEquals(scenarioName, statsRecorder.getRecordedScenario());
        assertEquals(transactionName, statsRecorder.getRecordedName());
        assertEquals("OK", statsRecorder.getRecordedStatus().name());

        // 7. Verify request was removed from store
        assertNull("Request should be removed after processing", requestStore.getRequest(correlationId));
    }

    @Test
    public void testMultipleRequestsWithDifferentCorrelationIds() throws InterruptedException {
        TestKit probe = new TestKit(system);
        ActorRef producerActor = system.actorOf(KafkaProducerActor.props(mockProducer, null, null));

        // Send multiple requests
        String[] correlationIds = { "corr-1", "corr-2", "corr-3" };
        for (String corrId : correlationIds) {
            String topic = "request-topic";
            String key = "key-" + corrId;
            byte[] value = ("value-" + corrId).getBytes();
            producerActor.tell(
                    new KafkaProducerActor.ProduceMessage(topic, key, value, corrId, true, null, null,
                            scala.collection.immutable.List$.MODULE$.empty()),
                    probe.getRef());
            requestStore.storeRequest(corrId, "key-" + corrId, ("value-" + corrId).getBytes(),
                    SerializationType.STRING, "Req-" + corrId, "Scenario", System.currentTimeMillis(), 30000);
        }

        Thread.sleep(100);

        // Verify all requests were sent
        assertEquals(3, mockProducer.history().size());

        // Process responses in different order
        // Process corr-2 first
        ConsumerRecord<String, Object> response2 = createResponseRecord("corr-2", "key-corr-2", "response-2");
        processor.process(Collections.singletonList(response2));
        assertNull("corr-2 should be removed", requestStore.getRequest("corr-2"));

        // Process corr-1
        ConsumerRecord<String, Object> response1 = createResponseRecord("corr-1", "key-corr-1", "response-1");
        processor.process(Collections.singletonList(response1));
        assertNull("corr-1 should be removed", requestStore.getRequest("corr-1"));

        // Verify corr-3 is still in store
        assertNotNull("corr-3 should still be in store", requestStore.getRequest("corr-3"));
    }

    @Test
    public void testInvalidCorrelationId() {
        // Create a response with correlation ID that doesn't exist in store
        ConsumerRecord<String, Object> responseRecord = createResponseRecord("unknown-corr-id", "key", "value");

        // Process should handle gracefully - it will trigger onUnmatched callback
        processor.process(Collections.singletonList(responseRecord));

        // Stats should still be recorded, but with KO status for unmatched
        assertTrue("Stats should be recorded even for unmatched", statsRecorder.wasResponseLogged());
        assertEquals("KO", statsRecorder.getRecordedStatus().name());
    }

    @Test
    public void testSerializationRoundTrip() throws InterruptedException {
        TestKit probe = new TestKit(system);
        String correlationId = "serialization-test";
        String key = "test-key";

        // Test with String serialization
        byte[] stringValue = "test string value".getBytes(StandardCharsets.UTF_8);
        requestStore.storeRequest(correlationId, key, stringValue, SerializationType.STRING,
                "StringTest", "TestScenario", System.currentTimeMillis(), 30000);

        ConsumerRecord<String, Object> response = createResponseRecord(correlationId, key, "response string");
        processor.process(Collections.singletonList(response));

        // Verify stats were recorded
        assertTrue("String serialization should be processed", statsRecorder.wasResponseLogged());
        assertEquals("OK", statsRecorder.getRecordedStatus().name());

        // Request should be removed after processing
        assertNull("Request should be removed", requestStore.getRequest(correlationId));
    }

    @Test
    public void testBatchProcessing() throws InterruptedException {
        TestKit probe = new TestKit(system);

        // Store multiple requests
        List<String> correlationIds = Arrays.asList("batch-1", "batch-2", "batch-3");
        for (String corrId : correlationIds) {
            requestStore.storeRequest(corrId, "key-" + corrId, ("value-" + corrId).getBytes(),
                    SerializationType.STRING, "BatchReq", "BatchScenario", System.currentTimeMillis(), 30000);
        }

        // Create batch of responses
        List<ConsumerRecord<String, Object>> responses = new ArrayList<>();
        for (String corrId : correlationIds) {
            responses.add(createResponseRecord(corrId, "key-" + corrId, "response-" + corrId));
        }

        // Process all in one batch
        processor.process(responses);

        // All should be removed
        for (String corrId : correlationIds) {
            assertNull("Request " + corrId + " should be removed", requestStore.getRequest(corrId));
        }
    }

    // Helper methods

    private ConsumerRecord<String, Object> createResponseRecord(String correlationId, String key, String value) {
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
                "response-topic", 0, 0L,
                key, (Object) value.getBytes(StandardCharsets.UTF_8));
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));
        return record;
    }

    private StatsEngine createStubStatsEngine() {
        return (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        statsRecorder.recordResponse(
                                (String) args[0], // scenario
                                (String) args[2], // name
                                (Status) args[5] // status
                        );
                    }
                    return null;
                });
    }

    // Helper class to record stats for verification
    private static class TestStatsRecorder {
        private boolean responseLogged = false;
        private String recordedScenario;
        private String recordedName;
        private Status recordedStatus;

        public void recordResponse(String scenario, String name, Status status) {
            this.responseLogged = true;
            this.recordedScenario = scenario;
            this.recordedName = name;
            this.recordedStatus = status;
        }

        public boolean wasResponseLogged() {
            return responseLogged;
        }

        public String getRecordedScenario() {
            return recordedScenario;
        }

        public String getRecordedName() {
            return recordedName;
        }

        public Status getRecordedStatus() {
            return recordedStatus;
        }
    }
}
