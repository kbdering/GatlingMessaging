package io.github.kbdering.kafka;

import io.gatling.commons.stats.Status;
import io.gatling.commons.util.Clock;
import io.gatling.core.stats.StatsEngine;
import io.github.kbdering.kafka.cache.BatchProcessor;
import io.github.kbdering.kafka.cache.RequestStore;
import io.github.kbdering.kafka.util.SerializationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;
import scala.Option;
import scala.collection.immutable.List$;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class MessageProcessorTimestampTest {

    @Test
    public void testProcessWithTimestampHeaderEnabled() {
        // Setup
        long messageTimestamp = 1234567890L;
        long clockTimestamp = 9876543210L;
        String correlationId = "test-correlation-id";
        String correlationHeaderName = "correlationId";

        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return clockTimestamp;
            }
        };

        AtomicLong capturedEndTime = new AtomicLong();

        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        // logResponse(String scenario, List<String> groups, String name, long start,
                        // long end, Status status, Option<String> responseCode, Option<String> message)
                        long end = (long) args[4];
                        capturedEndTime.set(end);
                        return null;
                    }
                    return null;
                });

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, Object> correlationIdToValue = (Map<String, Object>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];

                        if (correlationIdToValue.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, "1000");
                            requestData.put(RequestStore.TRANSACTION_NAME, "test-transaction");
                            requestData.put(RequestStore.SCENARIO_NAME, "test-scenario");
                            requestData.put(RequestStore.VALUE_BYTES, "request".getBytes(StandardCharsets.UTF_8));

                            processor.onMatch(correlationId, requestData, correlationIdToValue.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageProcessor messageProcessor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.emptyList(), (io.github.kbdering.kafka.extractors.CorrelationExtractor) null,
                correlationHeaderName, true);

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, messageTimestamp,
                TimestampType.CREATE_TIME, 0, 0, "key", "value",
                new RecordHeaders(Collections.singletonList(
                        new RecordHeader(correlationHeaderName, correlationId.getBytes(StandardCharsets.UTF_8)))),
                java.util.Optional.empty());

        // Execute
        messageProcessor.process(Collections.singletonList(record));

        // Verify
        assertEquals(messageTimestamp, capturedEndTime.get());
    }

    @Test
    public void testProcessWithTimestampHeaderDisabled() {
        // Setup
        long messageTimestamp = 1234567890L;
        long clockTimestamp = 9876543210L;
        String correlationId = "test-correlation-id";
        String correlationHeaderName = "correlationId";

        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return clockTimestamp;
            }
        };

        AtomicLong capturedEndTime = new AtomicLong();

        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        long end = (long) args[4];
                        capturedEndTime.set(end);
                        return null;
                    }
                    return null;
                });

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, Object> correlationIdToValue = (Map<String, Object>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];

                        if (correlationIdToValue.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, "1000");
                            requestData.put(RequestStore.TRANSACTION_NAME, "test-transaction");
                            requestData.put(RequestStore.SCENARIO_NAME, "test-scenario");
                            requestData.put(RequestStore.VALUE_BYTES, "request".getBytes(StandardCharsets.UTF_8));

                            processor.onMatch(correlationId, requestData, correlationIdToValue.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageProcessor messageProcessor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.emptyList(), (io.github.kbdering.kafka.extractors.CorrelationExtractor) null,
                correlationHeaderName, false);

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, messageTimestamp,
                TimestampType.CREATE_TIME, 0, 0, "key", "value",
                new RecordHeaders(Collections.singletonList(
                        new RecordHeader(correlationHeaderName, correlationId.getBytes(StandardCharsets.UTF_8)))),
                java.util.Optional.empty());

        // Execute
        messageProcessor.process(Collections.singletonList(record));

        // Verify
        assertEquals(clockTimestamp, capturedEndTime.get());
    }
}
