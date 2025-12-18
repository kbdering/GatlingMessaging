package pl.perfluencer.kafka.integration;

import io.gatling.commons.stats.Status;
import io.gatling.commons.util.Clock;
import io.gatling.core.stats.StatsEngine;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import pl.perfluencer.kafka.extractors.JsonPathExtractor;
import pl.perfluencer.kafka.extractors.KeyExtractor;
import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.kafka.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.MessageProcessor;
import pl.perfluencer.cache.RequestStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
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

public class CorrelationStrategyTest {

    private ActorSystem system;
    private RequestStore requestStore;
    private TestStatsRecorder statsRecorder;
    private StatsEngine stubStatsEngine;
    private Clock clock;

    @Before
    public void setUp() {
        system = ActorSystem.create("CorrelationStrategyTest");
        requestStore = new InMemoryRequestStore();
        statsRecorder = new TestStatsRecorder();
        clock = new Clock() {
            @Override
            public long nowMillis() {
                return System.currentTimeMillis();
            }
        };
        stubStatsEngine = createStubStatsEngine();
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
    public void testHeaderCorrelationStrategy() {
        // Default strategy: header "correlationId"
        MessageProcessor processor = createProcessor(null); // null extractor = use default header strategy

        String correlationId = "corr-header-1";
        // Store request
        requestStore.storeRequest(correlationId, "key", "val", SerializationType.STRING, "HeaderTx", "Scn",
                System.currentTimeMillis(), 10000);

        // Create response with header
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", "val");
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        // Process
        processor.process(Collections.singletonList(record));

        // Verify
        assertTrue("Header correlation should match", statsRecorder.wasResponseLogged());
        assertEquals("OK", statsRecorder.getRecordedStatus().name());
        assertNull(requestStore.getRequest(correlationId));
    }

    @Test
    public void testKeyCorrelationStrategy() {
        // Key strategy
        MessageProcessor processor = createProcessor(new KeyExtractor());

        String correlationId = "corr-key-1";
        // Store request
        requestStore.storeRequest(correlationId, "key", "val", SerializationType.STRING, "KeyTx", "Scn",
                System.currentTimeMillis(), 10000);

        // Create response with matching KEY
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, correlationId, "val");

        // Process
        processor.process(Collections.singletonList(record));

        // Verify
        assertTrue("Key correlation should match", statsRecorder.wasResponseLogged());
        assertEquals("OK", statsRecorder.getRecordedStatus().name());
        assertNull(requestStore.getRequest(correlationId));
    }

    @Test
    public void testKeyCorrelationMismatch() {
        // Key strategy
        MessageProcessor processor = createProcessor(new KeyExtractor());

        String correlationId = "corr-key-mismatch";
        // Store request
        requestStore.storeRequest(correlationId, "key", "val", SerializationType.STRING, "KeyTxMismatch", "Scn",
                System.currentTimeMillis(), 10000);

        // Create response with DIFFERENT key
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "wrong-key", "val");

        // Process
        processor.process(Collections.singletonList(record));

        // Verify
        assertTrue("Message should be logged as processed (even if unmatched)", statsRecorder.wasResponseLogged());
        assertEquals("Should be KO (unmatched)", "KO", statsRecorder.getRecordedStatus().name());
        assertNotNull("Request should still be in store", requestStore.getRequest(correlationId));
    }

    @Test
    public void testKeyCorrelationNullKey() {
        // Key strategy
        MessageProcessor processor = createProcessor(new KeyExtractor());

        String correlationId = "corr-key-null";
        // Store request
        requestStore.storeRequest(correlationId, "key", "val", SerializationType.STRING, "KeyTxNull", "Scn",
                System.currentTimeMillis(), 10000);

        // Create response with NULL key
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, null, "val");

        // Process
        processor.process(Collections.singletonList(record));

        // Verify
        // MessageProcessor ignores records with null correlation IDs
        assertFalse("Null key should NOT log response (ignored)", statsRecorder.wasResponseLogged());
        assertNotNull("Request should still be in store", requestStore.getRequest(correlationId));
    }

    @Test
    public void testJsonBodyCorrelationStrategy() {
        // Body strategy (JSON Path)
        // Assume response is JSON: {"requestId": "corr-json-1", "data": "foo"}
        MessageProcessor processor = createProcessor(new JsonPathExtractor("$.requestId"));

        String correlationId = "corr-json-1";
        // Store request
        requestStore.storeRequest(correlationId, "key", "val", SerializationType.STRING, "JsonTx", "Scn",
                System.currentTimeMillis(), 10000);

        // Create response with matching JSON body
        String jsonValue = "{\"requestId\": \"" + correlationId + "\", \"data\": \"foo\"}";
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", jsonValue);

        // Process
        processor.process(Collections.singletonList(record));

        // Verify
        assertTrue("JSON Body correlation should match", statsRecorder.wasResponseLogged());
        assertEquals("OK", statsRecorder.getRecordedStatus().name());
        assertNull(requestStore.getRequest(correlationId));
    }

    private MessageProcessor createProcessor(CorrelationExtractor extractor) {
        if (extractor == null) {
            extractor = new pl.perfluencer.kafka.extractors.HeaderExtractor("correlationId");
        }
        return new MessageProcessor(
                requestStore,
                stubStatsEngine,
                clock,
                ActorRef.noSender(),
                Collections.emptyList(),
                extractor,
                false);
    }

    // --- Helpers ---

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

        public Status getRecordedStatus() {
            return recordedStatus;
        }
    }
}
