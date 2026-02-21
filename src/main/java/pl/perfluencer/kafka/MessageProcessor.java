package pl.perfluencer.kafka;

import io.gatling.commons.stats.Status;
import io.gatling.core.stats.StatsEngine;
import io.gatling.commons.util.Clock;
import pl.perfluencer.cache.BatchProcessor;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.PoisonPill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.collection.immutable.List$;

import java.nio.charset.StandardCharsets;
import java.time.Duration; // Import Duration
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.DecoderFactory;

import pl.perfluencer.common.util.ParserRegistry;

public class MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private static final scala.collection.immutable.List<String> E2E_GROUP = scala.collection.immutable.List$.MODULE$
            .from(
                    scala.jdk.javaapi.CollectionConverters
                            .asScala(java.util.Collections.singletonList("End-to-End Group")));
    private final RequestStore requestStore;
    private final StatsEngine statsEngine;
    private final Clock clock;
    private final ActorRef controller;
    private final java.util.concurrent.ConcurrentMap<String, List<MessageCheck<?, ?>>> checkRegistry;
    private final List<MessageCheck<?, ?>> globalChecks;
    private final List<SessionAwareMessageCheck<?>> sessionAwareChecks;
    private final CorrelationExtractor correlationExtractor;
    private final boolean useTimestampHeader;

    private final Duration retryBackoff;
    private final int maxRetries;
    private final ParserRegistry parserRegistry;
    private final scala.collection.immutable.List<String> statsGroup;

    // Fallback cache for reflection when no registered parser exists
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, java.lang.reflect.Method> PROTOBUF_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    public MessageProcessor(RequestStore requestStore, StatsEngine statsEngine, Clock clock,
            ActorRef controller,
            List<MessageCheck<?, ?>> checks, CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader) {
        this(requestStore, statsEngine, clock, controller, new java.util.concurrent.ConcurrentHashMap<>(), checks, null,
                correlationExtractor, useTimestampHeader,
                Duration.ofMillis(50), 3, null, E2E_GROUP);
    }

    public MessageProcessor(RequestStore requestStore, StatsEngine statsEngine, Clock clock,
            ActorRef controller,
            List<MessageCheck<?, ?>> checks, List<SessionAwareMessageCheck<?>> sessionAwareChecks,
            CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader) {
        this(requestStore, statsEngine, clock, controller, new java.util.concurrent.ConcurrentHashMap<>(), checks,
                sessionAwareChecks, correlationExtractor,
                useTimestampHeader, Duration.ofMillis(50), 3, null, E2E_GROUP);
    }

    public MessageProcessor(RequestStore requestStore, StatsEngine statsEngine, Clock clock,
            ActorRef controller,
            java.util.concurrent.ConcurrentMap<String, List<MessageCheck<?, ?>>> checkRegistry,
            List<MessageCheck<?, ?>> globalChecks,
            List<SessionAwareMessageCheck<?>> sessionAwareChecks,
            CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader, Duration retryBackoff, int maxRetries) {
        this(requestStore, statsEngine, clock, controller, checkRegistry, globalChecks, sessionAwareChecks,
                correlationExtractor, useTimestampHeader, retryBackoff, maxRetries, null, E2E_GROUP);
    }

    public MessageProcessor(RequestStore requestStore, StatsEngine statsEngine, Clock clock,
            ActorRef controller,
            java.util.concurrent.ConcurrentMap<String, List<MessageCheck<?, ?>>> checkRegistry,
            List<MessageCheck<?, ?>> globalChecks,
            List<SessionAwareMessageCheck<?>> sessionAwareChecks,
            CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader, Duration retryBackoff, int maxRetries,
            ParserRegistry parserRegistry) {
        this(requestStore, statsEngine, clock, controller, checkRegistry, globalChecks, sessionAwareChecks,
                correlationExtractor, useTimestampHeader, retryBackoff, maxRetries, parserRegistry, E2E_GROUP);
    }

    public MessageProcessor(RequestStore requestStore, StatsEngine statsEngine, Clock clock,
            ActorRef controller,
            java.util.concurrent.ConcurrentMap<String, List<MessageCheck<?, ?>>> checkRegistry,
            List<MessageCheck<?, ?>> globalChecks,
            List<SessionAwareMessageCheck<?>> sessionAwareChecks,
            CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader, Duration retryBackoff, int maxRetries,
            ParserRegistry parserRegistry,
            scala.collection.immutable.List<String> statsGroup) {
        this.requestStore = requestStore;
        this.statsEngine = statsEngine;
        this.clock = clock;
        this.controller = controller;
        this.checkRegistry = checkRegistry;
        this.globalChecks = globalChecks != null ? globalChecks : java.util.Collections.emptyList();
        this.sessionAwareChecks = sessionAwareChecks;
        this.correlationExtractor = correlationExtractor;
        this.useTimestampHeader = useTimestampHeader;
        this.retryBackoff = retryBackoff;
        this.maxRetries = maxRetries;
        this.parserRegistry = parserRegistry;
        this.statsGroup = statsGroup;
    }

    public MessageProcessor(RequestStore requestStore, StatsEngine statsEngine, Clock clock,
            ActorRef controller,
            List<MessageCheck<?, ?>> checks, List<SessionAwareMessageCheck<?>> sessionAwareChecks,
            CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader, Duration retryBackoff, int maxRetries) {
        this(requestStore, statsEngine, clock, controller, new java.util.concurrent.ConcurrentHashMap<>(), checks,
                sessionAwareChecks, correlationExtractor, useTimestampHeader, retryBackoff, maxRetries, null,
                E2E_GROUP);
    }

    // Envelope to track retry attempts
    public static class RetryEnvelope {
        public final ConsumerRecord<String, Object> record;
        public final int attempts;

        public RetryEnvelope(ConsumerRecord<String, Object> record, int attempts) {
            this.record = record;
            this.attempts = attempts;
        }
    }

    public Duration getRetryBackoff() {
        return retryBackoff;
    }

    public List<RetryEnvelope> process(List<ConsumerRecord<String, Object>> records) {
        List<RetryEnvelope> envelopes = new java.util.ArrayList<>(records.size());
        for (ConsumerRecord<String, Object> record : records) {
            envelopes.add(new RetryEnvelope(record, 0));
        }
        return processRetries(envelopes);
    }

    /**
     * Process Kafka records directly from ConsumerRecords without intermediate List
     * allocation.
     * This is more efficient than process(List) as it avoids creating a temporary
     * ArrayList
     * in the consumer actor hot path.
     * 
     * @param records the ConsumerRecords from Kafka poll()
     * @return list of envelopes that need retry (unmatched messages)
     */
    public List<RetryEnvelope> process(org.apache.kafka.clients.consumer.ConsumerRecords<String, Object> records) {
        List<RetryEnvelope> envelopes = new java.util.ArrayList<>(records.count());
        for (ConsumerRecord<String, Object> record : records) {
            envelopes.add(new RetryEnvelope(record, 0));
        }
        return processRetries(envelopes);
    }

    public List<RetryEnvelope> processRetries(List<RetryEnvelope> records) {
        long defaultEndTime = clock.nowMillis();
        List<RetryEnvelope> retries = new java.util.ArrayList<>();

        // 1. Extract correlation IDs and map them to records (supporting duplicates)
        // Map correlationId -> List<RetryEnvelope>
        // Pre-size to avoid resizing during iteration (assume most IDs are unique)
        Map<String, List<RetryEnvelope>> correlationIdToValues = new HashMap<>(records.size());

        for (RetryEnvelope envelope : records) {
            String correlationId = null;
            if (correlationExtractor != null) {
                correlationId = correlationExtractor.extract(envelope.record);
            }

            if (correlationId != null) {
                correlationIdToValues.computeIfAbsent(correlationId, k -> new java.util.ArrayList<>())
                        .add(envelope);
            } else {
                // Cannot extract correlation ID -> Unmatched immediate failure?
                // Or treat as "Unmatched" which logs error anyway.
                // Let's wrap in a dummy list to processUnmatchedResponse logic handles it, or
                // just log here.
                // Existing logic ignored null correlationId entirely?
                // Looking at old code: "if (correlationId != null) ... else ..." -> it did
                // nothing if null.
                // So efficient skipping.
            }
        }

        if (correlationIdToValues.isEmpty()) {
            return retries;
        }

        // 2. Batch retrieve and remove requests from store
        try {
            // We cast because processBatchedRecords expects Map<String, Object>
            @SuppressWarnings("unchecked")
            Map<String, Object> opaqueMap = (Map<String, Object>) (Map<?, ?>) correlationIdToValues;

            requestStore.processBatchedRecords(opaqueMap, new BatchProcessor() {
                @Override
                public void onMatch(String correlationId, pl.perfluencer.cache.RequestData requestData,
                        Object responseValue) {
                    @SuppressWarnings("unchecked")
                    List<RetryEnvelope> envelopes = (List<RetryEnvelope>) responseValue;

                    if (!envelopes.isEmpty()) {
                        processSingleResponse(correlationId, requestData, envelopes.get(0).record, false);
                    }

                    for (int i = 1; i < envelopes.size(); i++) {
                        processSingleResponse(correlationId, requestData, envelopes.get(i).record, true);
                    }
                }

                @Override
                public void onUnmatched(String correlationId, Object responseValue) {
                    // responseValue is actually List<RetryEnvelope>
                    @SuppressWarnings("unchecked")
                    List<RetryEnvelope> envelopes = (List<RetryEnvelope>) responseValue;

                    for (RetryEnvelope env : envelopes) {
                        if (env.attempts < maxRetries) {
                            retries.add(new RetryEnvelope(env.record, env.attempts + 1));
                        } else {
                            processUnmatchedResponse(correlationId, env.record);
                        }
                    }
                }

                private void processSingleResponse(String correlationId, pl.perfluencer.cache.RequestData requestData,
                        ConsumerRecord<?, ?> record, boolean isDuplicate) {
                    long startTime = requestData.startTime;
                    String transactionName = requestData.transactionName;
                    String scenarioName = requestData.scenarioName;

                    long endTime = defaultEndTime;
                    Object actualResponseValue = record.value();

                    if (useTimestampHeader) {
                        endTime = record.timestamp();
                    }

                    Status status;
                    Option<String> errorMessage;
                    Option<String> responseCode;

                    if (isDuplicate) {
                        status = Status.apply("KO");
                        errorMessage = Option.apply("Duplicate response received");
                        responseCode = Option.apply("409"); // Conflict/Duplicate
                    } else {
                        // Default to OK
                        status = Status.apply("OK");
                        errorMessage = Option.empty();
                        responseCode = Option.apply("200"); // Simulate OK

                        // Run checks if any
                        List<MessageCheck<?, ?>> applicableChecks = new java.util.ArrayList<>(globalChecks);
                        if (checkRegistry != null) {
                            List<MessageCheck<?, ?>> registered = checkRegistry.get(transactionName);
                            if (registered != null) {
                                applicableChecks.addAll(registered);
                            }
                        }

                        if (applicableChecks != null && !applicableChecks.isEmpty()) {
                            for (MessageCheck<?, ?> check : applicableChecks) {
                                try {
                                    // Deserialize request
                                    Object request = null;
                                    if (check
                                            .getRequestSerdeType() == pl.perfluencer.common.util.SerializationType.STRING) {
                                        // RequestData.value is likely the raw object or byte[] depending on how it was
                                        // stored.
                                        // InMemoryRequestStore might have original object or byte[].
                                        if (requestData.value instanceof byte[]) {
                                            request = new String((byte[]) requestData.value, StandardCharsets.UTF_8);
                                        } else if (requestData.value instanceof String) {
                                            request = (String) requestData.value;
                                        } else {
                                            // If it's an object (like POJO from in-memory), toString?
                                            if (requestData.value != null)
                                                request = requestData.value.toString();
                                        }
                                    } else if (check
                                            .getRequestSerdeType() == pl.perfluencer.common.util.SerializationType.BYTE_ARRAY) {
                                        if (requestData.value instanceof byte[])
                                            request = (byte[]) requestData.value;
                                    } else if (check
                                            .getRequestSerdeType() == pl.perfluencer.common.util.SerializationType.PROTOBUF) {
                                        if (requestData.value instanceof byte[]) {
                                            request = MessageProcessor.this.deserializeProtobuf(
                                                    (byte[]) requestData.value,
                                                    check.getRequestClass());
                                        }
                                    } else if (check
                                            .getRequestSerdeType() == pl.perfluencer.common.util.SerializationType.AVRO) {
                                        if (check.getRequestClass().isInstance(requestData.value)) {
                                            // InMemoryRequestStore keeps the original object
                                            request = requestData.value;
                                        } else if (requestData.value instanceof byte[]) {
                                            request = MessageProcessor.this.deserializeAvro((byte[]) requestData.value,
                                                    check.getRequestClass());
                                        }
                                    }

                                    // Deserialize response
                                    Object response = null;
                                    if (check
                                            .getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.STRING) {
                                        if (actualResponseValue instanceof byte[]) {
                                            response = new String((byte[]) actualResponseValue, StandardCharsets.UTF_8);
                                        } else {
                                            response = actualResponseValue.toString();
                                        }
                                    } else if (check
                                            .getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.BYTE_ARRAY) {
                                        if (actualResponseValue instanceof byte[]) {
                                            response = actualResponseValue;
                                        } else {
                                            response = actualResponseValue.toString().getBytes(StandardCharsets.UTF_8);
                                        }
                                    } else if (check
                                            .getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.PROTOBUF) {
                                        if (check.getResponseClass().isInstance(actualResponseValue)) {
                                            response = actualResponseValue;
                                        } else {
                                            byte[] responseBytes = MessageProcessor.this
                                                    .extractBytes(actualResponseValue);
                                            if (responseBytes != null) {
                                                response = MessageProcessor.this.deserializeProtobuf(responseBytes,
                                                        check.getResponseClass());
                                            }
                                        }
                                    } else if (check
                                            .getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.AVRO) {
                                        if (check.getResponseClass().isInstance(actualResponseValue)) {
                                            response = actualResponseValue;
                                        } else {
                                            byte[] responseBytes = MessageProcessor.this
                                                    .extractBytes(actualResponseValue);
                                            if (responseBytes != null) {
                                                response = MessageProcessor.this.deserializeAvro(responseBytes,
                                                        check.getResponseClass());
                                            }
                                        }
                                    }

                                    // Execute check
                                    @SuppressWarnings("unchecked")
                                    java.util.function.BiFunction<Object, Object, Optional<String>> checkLogic = (java.util.function.BiFunction<Object, Object, Optional<String>>) check
                                            .getCheckLogic();

                                    Optional<String> failure = checkLogic.apply(request, response);
                                    if (failure.isPresent()) {
                                        status = Status.apply("KO");
                                        errorMessage = Option.apply(failure.get());
                                        responseCode = Option.apply("500"); // Check failure
                                        break;
                                    }

                                } catch (Exception e) {
                                    status = Status.apply("KO");
                                    errorMessage = Option.apply("Check execution failed: " + e.getMessage());
                                    responseCode = Option.apply("500");
                                    break;
                                }
                            }
                        }

                        // Run session-aware checks if any (and previous checks passed)
                        if (status.name().equals("OK") && sessionAwareChecks != null && !sessionAwareChecks.isEmpty()) {
                            Map<String, String> sessionVariables = requestData.sessionVariables;
                            if (sessionVariables != null) {
                                for (SessionAwareMessageCheck<?> check : sessionAwareChecks) {
                                    try {
                                        // Deserialize response
                                        Object response = null;
                                        if (check
                                                .getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.STRING) {
                                            if (actualResponseValue instanceof byte[]) {
                                                response = new String((byte[]) actualResponseValue,
                                                        StandardCharsets.UTF_8);
                                            } else {
                                                response = actualResponseValue.toString();
                                            }
                                        } else if (check
                                                .getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.BYTE_ARRAY) {
                                            if (actualResponseValue instanceof byte[]) {
                                                response = actualResponseValue;
                                            } else {
                                                response = actualResponseValue.toString()
                                                        .getBytes(StandardCharsets.UTF_8);
                                            }
                                        }

                                        Optional<String> failure = check.validate(sessionVariables, response);
                                        if (failure.isPresent()) {
                                            status = Status.apply("KO");
                                            errorMessage = Option.apply(failure.get());
                                            responseCode = Option.apply("500");
                                            break;
                                        }
                                    } catch (Exception e) {
                                        status = Status.apply("KO");
                                        errorMessage = Option.apply("Session-aware check failed: " + e.getMessage());
                                        responseCode = Option.apply("500");
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    statsEngine.logResponse(
                            scenarioName,
                            MessageProcessor.this.statsGroup, // groups
                            transactionName, // name
                            startTime,
                            endTime,
                            status,
                            responseCode,
                            errorMessage);

                    statsEngine.logGroupEnd(
                            scenarioName,
                            new io.gatling.core.session.GroupBlock(MessageProcessor.this.statsGroup, startTime,
                                    (int) (endTime - startTime), status),
                            endTime);
                }

                private void processUnmatchedResponse(String correlationId, ConsumerRecord<?, ?> record) {
                    long startTime = defaultEndTime; // Default start time if lookup fails early
                    String transactionName = "Missing Match"; // Default transaction name
                    String scenarioName = "Unknown Scenario"; // Default scenario name
                    Status status = Status.apply("KO"); // Default status is failure
                    Option<String> errorMessage = Option.apply("Request data not found for correlationId");
                    Option<String> responseCode = Option.apply("404"); // Simulate Not Found

                    long endTime = defaultEndTime;
                    if (useTimestampHeader) {
                        endTime = record.timestamp();
                        startTime = endTime;
                    }

                    statsEngine.logResponse(
                            scenarioName,
                            MessageProcessor.this.statsGroup,
                            transactionName,
                            startTime,
                            endTime,
                            status,
                            responseCode,
                            errorMessage);

                    statsEngine.logGroupEnd(
                            scenarioName,
                            new io.gatling.core.session.GroupBlock(MessageProcessor.this.statsGroup, startTime,
                                    (int) (endTime - startTime), status),
                            endTime);
                }
            });
        } catch (Exception e) {
            // CRITICAL: Stop the test if RequestStore retrieval fails (e.g., Redis down)
            if (controller != null) {
                logger.error("Critical error in MessageProcessor. Sending PoisonPill to Controller.", e);
                controller.tell(PoisonPill.getInstance(), ActorRef.noSender());
            } else {
                throw new RuntimeException("Error accessing RequestStore and no Controller available to stop test", e);
            }
        }

        return retries;
    }

    /**
     * Process consumed records without request correlation (consume-only mode).
     * Applies response-only checks to every record and reports stats directly.
     *
     * @param records            the consumed records
     * @param consumeRequestName the request name used for stats and check registry
     *                           lookup
     * @param scenarioName       the scenario name for stats reporting
     */
    public void processConsumeOnly(org.apache.kafka.clients.consumer.ConsumerRecords<String, Object> records,
            String consumeRequestName, String scenarioName) {
        long endTime = clock.nowMillis();

        List<MessageCheck<?, ?>> applicableChecks = new java.util.ArrayList<>(globalChecks);
        if (checkRegistry != null) {
            List<MessageCheck<?, ?>> registered = checkRegistry.get(consumeRequestName);
            if (registered != null) {
                applicableChecks.addAll(registered);
            }
        }

        for (ConsumerRecord<String, Object> record : records) {
            long startTime = endTime; // For consume-only, start = receive time
            if (useTimestampHeader) {
                startTime = record.timestamp();
            }

            Status status = Status.apply("OK");
            Option<String> errorMessage = Option.empty();
            Option<String> responseCode = Option.apply("200");

            Object actualResponseValue = record.value();

            for (MessageCheck<?, ?> check : applicableChecks) {
                try {
                    // Deserialize response
                    Object response = null;
                    if (check.getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.STRING) {
                        if (actualResponseValue instanceof byte[]) {
                            response = new String((byte[]) actualResponseValue, StandardCharsets.UTF_8);
                        } else {
                            response = actualResponseValue.toString();
                        }
                    } else if (check
                            .getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.BYTE_ARRAY) {
                        if (actualResponseValue instanceof byte[]) {
                            response = actualResponseValue;
                        } else {
                            response = actualResponseValue.toString().getBytes(StandardCharsets.UTF_8);
                        }
                    } else if (check.getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.AVRO) {
                        if (check.getResponseClass().isInstance(actualResponseValue)) {
                            response = actualResponseValue;
                        } else {
                            byte[] responseBytes = extractBytes(actualResponseValue);
                            if (responseBytes != null) {
                                response = deserializeAvro(responseBytes, check.getResponseClass());
                            }
                        }
                    } else if (check.getResponseSerdeType() == pl.perfluencer.common.util.SerializationType.PROTOBUF) {
                        if (check.getResponseClass().isInstance(actualResponseValue)) {
                            response = actualResponseValue;
                        } else {
                            byte[] responseBytes = extractBytes(actualResponseValue);
                            if (responseBytes != null) {
                                response = deserializeProtobuf(responseBytes, check.getResponseClass());
                            }
                        }
                    }

                    // Execute check — request is null in consume-only mode
                    @SuppressWarnings("unchecked")
                    java.util.function.BiFunction<Object, Object, Optional<String>> checkLogic = (java.util.function.BiFunction<Object, Object, Optional<String>>) check
                            .getCheckLogic();

                    Optional<String> failure = checkLogic.apply(null, response);
                    if (failure.isPresent()) {
                        status = Status.apply("KO");
                        errorMessage = Option.apply(failure.get());
                        responseCode = Option.apply("500");
                        break;
                    }
                } catch (Exception e) {
                    status = Status.apply("KO");
                    errorMessage = Option.apply("Check execution failed: " + e.getMessage());
                    responseCode = Option.apply("500");
                    break;
                }
            }

            statsEngine.logResponse(
                    scenarioName,
                    this.statsGroup,
                    consumeRequestName,
                    startTime,
                    endTime,
                    status,
                    responseCode,
                    errorMessage);

            statsEngine.logGroupEnd(
                    scenarioName,
                    new io.gatling.core.session.GroupBlock(this.statsGroup, startTime, (int) (endTime - startTime),
                            status),
                    endTime);
        }
    }

    private byte[] extractBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    private Object deserializeProtobuf(byte[] data, Class<?> clazz) {
        // Fast path: use registered parser (zero reflection)
        if (parserRegistry != null) {
            Object result = parserRegistry.parse(clazz, data);
            if (result != null) {
                return result;
            }
        }

        // Fallback: cached reflection (Method lookup done once per class)
        try {
            java.lang.reflect.Method parseFrom = PROTOBUF_METHOD_CACHE.computeIfAbsent(clazz, c -> {
                try {
                    return c.getMethod("parseFrom", byte[].class);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Not a valid Protobuf class: " + c.getName(), e);
                }
            });
            return parseFrom.invoke(null, data);
        } catch (Exception e) {
            logger.error("Failed to deserialize Protobuf message of type " + clazz.getName(), e);
            throw new RuntimeException("Protobuf deserialization failed", e);
        }
    }

    private Object deserializeAvro(byte[] data, Class<?> clazz) {
        try {
            // Assume SpecificRecord
            SpecificDatumReader<?> reader = new SpecificDatumReader<>(clazz);
            return reader.read(null, DecoderFactory.get().binaryDecoder(data, null));
        } catch (Exception e) {
            logger.error("Failed to deserialize Avro message of type " + clazz.getName(), e);
            throw new RuntimeException("Avro deserialization failed", e);
        }
    }
}
