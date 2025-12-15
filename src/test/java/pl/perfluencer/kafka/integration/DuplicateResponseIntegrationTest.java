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

public class DuplicateResponseIntegrationTest {

    private ActorSystem system;
    private MockProducer<String, Object> mockProducer;
    private MockConsumer<String, Object> mockConsumer;
    private RequestStore requestStore;
    private StatsEngine stubStatsEngine;
    private TestStatsRecorder statsRecorder;
    private MessageProcessor processor;

    @Before
    public void setUp() {
        system = ActorSystem.create("DuplicateResponseIntegrationTest");
        @SuppressWarnings("unchecked")
        org.apache.kafka.common.serialization.Serializer<Object> valueSerializer = (org.apache.kafka.common.serialization.Serializer<Object>) (org.apache.kafka.common.serialization.Serializer<?>) new ByteArraySerializer();
        mockProducer = new MockProducer<>(true, new StringSerializer(), valueSerializer);
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        requestStore = new InMemoryRequestStore();
        statsRecorder = new TestStatsRecorder();
        stubStatsEngine = createStubStatsEngine();

        Clock clock = () -> System.currentTimeMillis();
        List<MessageCheck<?, ?>> checks = Collections.emptyList();
        CorrelationExtractor correlationExtractor = new pl.perfluencer.kafka.extractors.HeaderExtractor(
                "correlationId");
        processor = new MessageProcessor(requestStore, stubStatsEngine, clock, ActorRef.noSender(), checks,
                correlationExtractor,
                false);

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
    public void testDuplicateResponseInSingleBatch() throws InterruptedException {
        String correlationId = "duplicate-test-id";
        String requestKey = "req-key";
        byte[] requestValue = "req-val".getBytes();
        String transactionName = "DuplicateTest";

        // 1. Store the request
        requestStore.storeRequest(correlationId, requestKey, requestValue, SerializationType.STRING,
                transactionName, "Scenario", System.currentTimeMillis(), 30000);

        // 2. Create TWO identical response records
        ConsumerRecord<String, Object> record1 = createResponseRecord(correlationId, requestKey, "resp-1");
        ConsumerRecord<String, Object> record2 = createResponseRecord(correlationId, requestKey, "resp-1"); // Duplicate

        // 3. Process them in a SINGLE batch
        List<ConsumerRecord<String, Object>> batch = Arrays.asList(record1, record2);
        processor.process(batch);

        // 4. Verify results
        // Assert we captured 2 events (1 success, 1 duplicate failure)
        assertEquals("Should log 2 events (1 success, 1 failure)", 2, statsRecorder.getEvents().size());

        // Event 1: Success
        TestStatsRecorder.RecordedEvent event1 = statsRecorder.getEvents().get(0);
        assertEquals("Event should be OK", "OK", event1.status.name());
        assertEquals("Event name match", transactionName, event1.name);

        // Event 2: Failure (Duplicate)
        TestStatsRecorder.RecordedEvent event2 = statsRecorder.getEvents().get(1);
        assertEquals("Second event should be KO (duplicate)", "KO", event2.status.name());
        assertEquals("Second event should have correct name", transactionName, event2.name);
        assertTrue("Second event should show duplicate error",
                event2.errorMessage.contains("Duplicate response received"));

        // 5. Verify Store is empty
        assertNull("Request should be removed", requestStore.getRequest(correlationId));
    }

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
                        String errorMessage = "";
                        if (args.length > 7 && args[7] instanceof scala.Option) {
                            scala.Option<?> opt = (scala.Option<?>) args[7];
                            if (opt.isDefined())
                                errorMessage = opt.get().toString();
                        }

                        statsRecorder.recordResponse(
                                (String) args[0], // scenario
                                (String) args[2], // name
                                (Status) args[5], // status
                                errorMessage);
                    }
                    return null;
                });
    }

    private static class TestStatsRecorder {
        static class RecordedEvent {
            String scenario;
            String name;
            Status status;
            String errorMessage;

            RecordedEvent(String scenario, String name, Status status, String errorMessage) {
                this.scenario = scenario;
                this.name = name;
                this.status = status;
                this.errorMessage = errorMessage;
            }
        }

        private final List<RecordedEvent> events = new ArrayList<>();

        public void recordResponse(String scenario, String name, Status status, String errorMessage) {
            events.add(new RecordedEvent(scenario, name, status, errorMessage));
        }

        public List<RecordedEvent> getEvents() {
            return events;
        }
    }
}
