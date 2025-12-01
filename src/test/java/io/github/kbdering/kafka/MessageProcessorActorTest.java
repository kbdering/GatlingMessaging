package io.github.kbdering.kafka;

import org.apache.pekko.actor.ActorSystem;
import io.gatling.commons.stats.Status;
import io.gatling.core.stats.StatsEngine;
import io.gatling.commons.util.Clock;
import io.github.kbdering.kafka.cache.RequestStore;
import io.github.kbdering.kafka.cache.BatchProcessor;
import io.github.kbdering.kafka.util.SerializationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

public class MessageProcessorActorTest {

    private ActorSystem system;

    @Before
    public void setUp() {
        system = ActorSystem.create();
    }

    @After
    public void tearDown() {
        system.terminate();
    }

    @Test
    public void testProcessMatchedRequest() {
        long startTime = 1000L;
        long endTime = 1100L;
        String correlationId = "corr-1";
        String transactionName = "test-req";
        String scenarioName = "test-scenario";

        // Manual Stub for Clock
        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return endTime;
            }
        };

        // Proxy for StatsEngine
        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        // logResponse(String scenario, List<String> groups, String name, long start,
                        // long end, Status status, Option<String> responseCode, Option<String> message)
                        String scenario = (String) args[0];
                        String name = (String) args[2];
                        long start = (long) args[3];
                        long end = (long) args[4];
                        Status status = (Status) args[5];

                        assertEquals(scenarioName, scenario);
                        assertEquals(transactionName, name);
                        assertEquals(startTime, start);
                        assertEquals(endTime, end);
                        assertEquals("OK", status.name());
                        return null;
                    }
                    return null; // Return null for other methods
                });

        // Proxy for RequestStore
        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, byte[]> records = (Map<String, byte[]>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];
                        if (records.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));
                            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
                            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
                            requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.STRING);
                            requestData.put(RequestStore.VALUE_BYTES, "request".getBytes());

                            processor.onMatch(correlationId, requestData, records.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.emptyList(), null, "correlationId");

        // Prepare input record
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", (Object) "response");
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        // Execute
        processor.process(Collections.singletonList(record));
    }

    @Test
    public void testProcessUnmatchedRequest() {
        // Setup with empty store (no match)
        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> null);

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> null);

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, new Clock() {
            @Override
            public long nowMillis() {
                return 0;
            }
        }, Collections.emptyList(), null, "correlationId");

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", (Object) "response");
        record.headers().add(new RecordHeader("correlationId", "unknown-id".getBytes(StandardCharsets.UTF_8)));

        // Should not crash
        processor.process(Collections.singletonList(record));
    }

    @Test
    public void testProcessMalformedRequest() {
        // Setup
        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> null);

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> null);

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, new Clock() {
            @Override
            public long nowMillis() {
                return 0;
            }
        }, Collections.emptyList(), null, "correlationId");

        // Record with missing correlationId header
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", (Object) "response");

        // Should not crash and should log warning (verified by absence of exception)
        processor.process(Collections.singletonList(record));
    }

    @Test
    public void testProcessMatchedRequestWithStringCheck() {
        long startTime = 1000L;
        long endTime = 1100L;
        String correlationId = "corr-string";
        String transactionName = "test-req-string";
        String scenarioName = "test-scenario-string";

        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return endTime;
            }
        };

        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        Status status = (Status) args[5];
                        assertEquals("OK", status.name());
                        return null;
                    }
                    return null;
                });

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, byte[]> records = (Map<String, byte[]>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];
                        if (records.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));
                            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
                            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
                            requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.STRING);
                            requestData.put(RequestStore.VALUE_BYTES, "request".getBytes(StandardCharsets.UTF_8));

                            processor.onMatch(correlationId, requestData, records.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageCheck<String, String> stringCheck = new MessageCheck<>(
                "string check",
                String.class, SerializationType.STRING,
                String.class, SerializationType.STRING,
                (req, res) -> {
                    if ("request".equals(req) && "response".equals(res)) {
                        return Optional.empty();
                    } else {
                        return Optional.of("Check failed");
                    }
                });

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.singletonList(stringCheck), null, "correlationId");

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", (Object) "response");
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        processor.process(Collections.singletonList(record));
    }

    @Test
    public void testProcessMatchedRequestWithByteArrayCheck() {
        long startTime = 1000L;
        long endTime = 1100L;
        String correlationId = "corr-bytes";
        String transactionName = "test-req-bytes";
        String scenarioName = "test-scenario-bytes";

        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return endTime;
            }
        };

        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        Status status = (Status) args[5];
                        assertEquals("OK", status.name());
                        return null;
                    }
                    return null;
                });

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, byte[]> records = (Map<String, byte[]>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];
                        if (records.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));
                            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
                            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
                            requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.BYTE_ARRAY);
                            requestData.put(RequestStore.VALUE_BYTES, new byte[] { 1, 2, 3 });

                            processor.onMatch(correlationId, requestData, records.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageCheck<byte[], byte[]> bytesCheck = new MessageCheck<>(
                "bytes check",
                byte[].class, SerializationType.BYTE_ARRAY,
                byte[].class, SerializationType.BYTE_ARRAY,
                (req, res) -> {
                    if (req.length == 3 && res.length == 3 && req[0] == 1 && res[0] == 4) {
                        return Optional.empty();
                    } else {
                        return Optional.of("Check failed");
                    }
                });

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.singletonList(bytesCheck), null, "correlationId");

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key",
                (Object) new byte[] { 4, 5, 6 });
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        processor.process(Collections.singletonList(record));
    }

    @Test
    public void testProcessMatchedRequestWithFailedCheck() {
        long startTime = 1000L;
        long endTime = 1100L;
        String correlationId = "corr-fail";
        String transactionName = "test-req-fail";
        String scenarioName = "test-scenario-fail";

        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return endTime;
            }
        };

        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        Status status = (Status) args[5];
                        scala.Option<String> message = (scala.Option<String>) args[7];

                        assertEquals("KO", status.name());
                        assertEquals("Intentional failure", message.get());
                        return null;
                    }
                    return null;
                });

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, byte[]> records = (Map<String, byte[]>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];
                        if (records.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));
                            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
                            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
                            requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.STRING);
                            requestData.put(RequestStore.VALUE_BYTES, "request".getBytes(StandardCharsets.UTF_8));

                            processor.onMatch(correlationId, requestData, records.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageCheck<String, String> failCheck = new MessageCheck<>(
                "fail check",
                String.class, SerializationType.STRING,
                String.class, SerializationType.STRING,
                (req, res) -> Optional.of("Intentional failure"));

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.singletonList(failCheck), null, "correlationId");

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", (Object) "response");
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        processor.process(Collections.singletonList(record));
    }

    @Test
    public void testProcessMatchedRequestWithSpecialChars() {
        long startTime = 1000L;
        long endTime = 1100L;
        String correlationId = "corr-special";
        String transactionName = "test-req-special";
        String scenarioName = "test-scenario-special";
        String specialString = "Hello world! \uD83D\uDE00"; // "Hello world! 😀"

        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return endTime;
            }
        };

        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        Status status = (Status) args[5];
                        assertEquals("OK", status.name());
                        return null;
                    }
                    return null;
                });

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, byte[]> records = (Map<String, byte[]>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];
                        if (records.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));
                            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
                            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
                            requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.STRING);
                            requestData.put(RequestStore.VALUE_BYTES, specialString.getBytes(StandardCharsets.UTF_8));

                            processor.onMatch(correlationId, requestData, records.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageCheck<String, String> specialCheck = new MessageCheck<>(
                "special check",
                String.class, SerializationType.STRING,
                String.class, SerializationType.STRING,
                (req, res) -> {
                    if (specialString.equals(req) && specialString.equals(res)) {
                        return Optional.empty();
                    } else {
                        return Optional.of("Check failed: expected " + specialString + " but got " + res);
                    }
                });

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.singletonList(specialCheck), null, "correlationId");

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", (Object) specialString);
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        processor.process(Collections.singletonList(record));
    }

    @Test
    public void testProcessMatchedRequestWithMismatchedEncoding() {
        long startTime = 1000L;
        long endTime = 1100L;
        String correlationId = "corr-encoding";
        String transactionName = "test-req-encoding";
        String scenarioName = "test-scenario-encoding";
        String originalString = "Hello";
        byte[] utf16Bytes = originalString.getBytes(StandardCharsets.UTF_16);

        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return endTime;
            }
        };

        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        Status status = (Status) args[5];
                        scala.Option<String> message = (scala.Option<String>) args[7];

                        assertEquals("KO", status.name());
                        // The check should fail because UTF-16 bytes interpreted as UTF-8 won't match
                        // "Hello"
                        assertEquals("Check failed", message.get());
                        return null;
                    }
                    return null;
                });

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, byte[]> records = (Map<String, byte[]>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];
                        if (records.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));
                            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
                            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
                            requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.STRING);
                            requestData.put(RequestStore.VALUE_BYTES, originalString.getBytes(StandardCharsets.UTF_8));

                            processor.onMatch(correlationId, requestData, records.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageCheck<String, String> encodingCheck = new MessageCheck<>(
                "encoding check",
                String.class, SerializationType.STRING,
                String.class, SerializationType.STRING,
                (req, res) -> {
                    if (originalString.equals(res)) {
                        return Optional.empty();
                    } else {
                        return Optional.of("Check failed");
                    }
                });

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.singletonList(encodingCheck), null, "correlationId");

        // Send response encoded in UTF-16, but processor expects UTF-8 (default for
        // STRING)
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", (Object) utf16Bytes);
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        processor.process(Collections.singletonList(record));
    }

    @Test
    public void testProcessMatchedRequestWithMalformedData() {
        long startTime = 1000L;
        long endTime = 1100L;
        String correlationId = "corr-malformed";
        String transactionName = "test-req-malformed";
        String scenarioName = "test-scenario-malformed";

        Clock stubClock = new Clock() {
            @Override
            public long nowMillis() {
                return endTime;
            }
        };

        StatsEngine stubStatsEngine = (StatsEngine) java.lang.reflect.Proxy.newProxyInstance(
                StatsEngine.class.getClassLoader(),
                new Class[] { StatsEngine.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("logResponse")) {
                        Status status = (Status) args[5];
                        scala.Option<String> message = (scala.Option<String>) args[7];

                        assertEquals("KO", status.name());
                        assertEquals("Check failed", message.get());
                        return null;
                    }
                    return null;
                });

        RequestStore stubRequestStore = (RequestStore) java.lang.reflect.Proxy.newProxyInstance(
                RequestStore.class.getClassLoader(),
                new Class[] { RequestStore.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("processBatchedRecords")) {
                        Map<String, byte[]> records = (Map<String, byte[]>) args[0];
                        BatchProcessor processor = (BatchProcessor) args[1];
                        if (records.containsKey(correlationId)) {
                            Map<String, Object> requestData = new HashMap<>();
                            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));
                            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
                            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
                            requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.STRING);
                            requestData.put(RequestStore.VALUE_BYTES, "request".getBytes(StandardCharsets.UTF_8));

                            processor.onMatch(correlationId, requestData, records.get(correlationId));
                        }
                        return null;
                    }
                    return null;
                });

        MessageCheck<String, String> malformedCheck = new MessageCheck<>(
                "malformed check",
                String.class, SerializationType.STRING,
                String.class, SerializationType.STRING,
                (req, res) -> {
                    // Expecting a specific valid string, but will get replacement chars
                    if ("valid".equals(res)) {
                        return Optional.empty();
                    } else {
                        return Optional.of("Check failed");
                    }
                });

        MessageProcessor processor = new MessageProcessor(stubRequestStore, stubStatsEngine, stubClock,
                Collections.singletonList(malformedCheck), null, "correlationId");

        // Invalid UTF-8 bytes
        byte[] malformedBytes = new byte[] { (byte) 0xFF, (byte) 0xFF };
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", (Object) malformedBytes);
        record.headers().add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        processor.process(Collections.singletonList(record));
    }
}
