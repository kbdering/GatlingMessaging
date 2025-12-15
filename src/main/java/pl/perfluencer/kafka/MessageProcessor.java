package pl.perfluencer.kafka;

import io.gatling.commons.stats.Status;
import io.gatling.core.stats.StatsEngine;
import io.gatling.commons.util.Clock;
import pl.perfluencer.kafka.cache.BatchProcessor;
import pl.perfluencer.kafka.cache.RequestStore;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.PoisonPill;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.collection.immutable.List$;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private final RequestStore requestStore;
    private final StatsEngine statsEngine;
    private final Clock clock;
    private final ActorRef controller;
    private final List<MessageCheck<?, ?>> checks;
    private final CorrelationExtractor correlationExtractor;
    private final boolean useTimestampHeader;

    public MessageProcessor(RequestStore requestStore, StatsEngine statsEngine, Clock clock,
            ActorRef controller,
            List<MessageCheck<?, ?>> checks, CorrelationExtractor correlationExtractor,
            boolean useTimestampHeader) {
        this.requestStore = requestStore;
        this.statsEngine = statsEngine;
        this.clock = clock;
        this.controller = controller;
        this.checks = checks;
        this.correlationExtractor = correlationExtractor;
        this.useTimestampHeader = useTimestampHeader;
    }

    public void process(List<ConsumerRecord<String, Object>> records) {
        long defaultEndTime = clock.nowMillis();

        // 1. Extract correlation IDs and map them to records (supporting duplicates)
        Map<String, List<Object>> correlationIdToValues = new HashMap<>();
        for (ConsumerRecord<String, Object> record : records) {
            String correlationId = null;

            if (correlationExtractor != null) {
                correlationId = correlationExtractor.extract(record);
            }

            if (correlationId != null) {
                Object valueToStore;
                if (useTimestampHeader) {
                    valueToStore = record;
                } else {
                    valueToStore = record.value();
                }
                correlationIdToValues.computeIfAbsent(correlationId, k -> new java.util.ArrayList<>())
                        .add(valueToStore);
            }
        }

        // 2. Batch retrieve and remove requests from store
        try {
            // We cast the map because processBatchedRecords expects Map<String, Object> but
            // passes values through opaque
            @SuppressWarnings("unchecked")
            Map<String, Object> opaqueMap = (Map<String, Object>) (Map<?, ?>) correlationIdToValues;

            requestStore.processBatchedRecords(opaqueMap, new BatchProcessor() {
                @Override
                public void onMatch(String correlationId, Map<String, Object> requestData, Object responseValue) {
                    // responseValue is actually List<Object>
                    List<?> values = (List<?>) responseValue;

                    // Process first response as SUCCESS
                    if (!values.isEmpty()) {
                        processSingleResponse(correlationId, requestData, values.get(0), false);
                    }

                    // Process subsequent responses as DUPLICATES (Error)
                    for (int i = 1; i < values.size(); i++) {
                        processSingleResponse(correlationId, requestData, values.get(i), true);
                    }
                }

                @Override
                public void onUnmatched(String correlationId, Object responseValue) {
                    // responseValue is actually List<Object>
                    List<?> values = (List<?>) responseValue;

                    for (Object val : values) {
                        processUnmatchedResponse(correlationId, val);
                    }
                }

                private void processSingleResponse(String correlationId, Map<String, Object> requestData,
                        Object responseValue, boolean isDuplicate) {
                    long startTime = Long.parseLong((String) requestData.get(RequestStore.START_TIME));
                    String transactionName = (String) requestData.get(RequestStore.TRANSACTION_NAME);
                    String scenarioName = (String) requestData.get(RequestStore.SCENARIO_NAME);

                    long endTime = defaultEndTime;
                    Object actualResponseValue = responseValue;

                    if (useTimestampHeader && responseValue instanceof ConsumerRecord) {
                        ConsumerRecord<?, ?> record = (ConsumerRecord<?, ?>) responseValue;
                        endTime = record.timestamp();
                        actualResponseValue = record.value();
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
                        if (checks != null && !checks.isEmpty()) {
                            for (MessageCheck<?, ?> check : checks) {
                                try {
                                    // Deserialize request
                                    Object request = null;
                                    if (check
                                            .getRequestSerdeType() == pl.perfluencer.kafka.util.SerializationType.STRING) {
                                        byte[] requestBytes = (byte[]) requestData.get(RequestStore.VALUE_BYTES);
                                        if (requestBytes != null) {
                                            request = new String(requestBytes, StandardCharsets.UTF_8);
                                        }
                                    } else if (check
                                            .getRequestSerdeType() == pl.perfluencer.kafka.util.SerializationType.BYTE_ARRAY) {
                                        request = (byte[]) requestData.get(RequestStore.VALUE_BYTES);
                                    }

                                    // Deserialize response
                                    Object response = null;
                                    if (check
                                            .getResponseSerdeType() == pl.perfluencer.kafka.util.SerializationType.STRING) {
                                        if (actualResponseValue instanceof byte[]) {
                                            response = new String((byte[]) actualResponseValue, StandardCharsets.UTF_8);
                                        } else {
                                            response = actualResponseValue.toString();
                                        }
                                    } else if (check
                                            .getResponseSerdeType() == pl.perfluencer.kafka.util.SerializationType.BYTE_ARRAY) {
                                        if (actualResponseValue instanceof byte[]) {
                                            response = actualResponseValue;
                                        } else {
                                            response = actualResponseValue.toString().getBytes(StandardCharsets.UTF_8);
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
                    }

                    statsEngine.logResponse(
                            scenarioName,
                            List$.MODULE$.empty(), // groups
                            transactionName, // name
                            startTime,
                            endTime,
                            status,
                            responseCode,
                            errorMessage);
                }

                private void processUnmatchedResponse(String correlationId, Object responseValue) {
                    long startTime = defaultEndTime; // Default start time if lookup fails early
                    String transactionName = "Missing Match"; // Default transaction name
                    String scenarioName = "Unknown Scenario"; // Default scenario name
                    Status status = Status.apply("KO"); // Default status is failure
                    Option<String> errorMessage = Option.apply("Request data not found for correlationId");
                    Option<String> responseCode = Option.apply("404"); // Simulate Not Found

                    long endTime = defaultEndTime;
                    if (useTimestampHeader && responseValue instanceof ConsumerRecord) {
                        ConsumerRecord<?, ?> record = (ConsumerRecord<?, ?>) responseValue;
                        endTime = record.timestamp();
                        startTime = endTime;
                    }

                    statsEngine.logResponse(
                            scenarioName,
                            List$.MODULE$.empty(),
                            transactionName,
                            startTime,
                            endTime,
                            status,
                            responseCode,
                            errorMessage);
                }
            });
        } catch (Exception e) {
            // CRITICAL: Stop the test if RequestStore retrieval fails (e.g., Redis down)
            if (controller != null) {
                logger.error("Critical error in MessageProcessor. Sending PoisonPill to Controller.", e);
                // Send PoisonPill to stop the Controller (and thus the test) without using
                // internal Crash class
                controller.tell(PoisonPill.getInstance(), ActorRef.noSender());
            } else {
                // Should not happen in real execution, but good for safety
                throw new RuntimeException("Error accessing RequestStore and no Controller available to stop test", e);
            }
        }
    }
}
