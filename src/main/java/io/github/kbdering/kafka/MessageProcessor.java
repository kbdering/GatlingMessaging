package io.github.kbdering.kafka;

import io.gatling.commons.stats.Status;
import io.gatling.core.stats.StatsEngine;
import io.gatling.commons.util.Clock;
import io.github.kbdering.kafka.cache.BatchProcessor;
import io.github.kbdering.kafka.cache.RequestStore;
import io.github.kbdering.kafka.extractors.CorrelationExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import scala.Option;
import scala.collection.immutable.List$;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MessageProcessor {

    private final RequestStore requestStore;
    private final StatsEngine statsEngine;
    private final Clock clock;
    private final List<MessageCheck<?, ?>> checks;
    private final CorrelationExtractor correlationExtractor;
    private final String correlationHeaderName;
    private final boolean useTimestampHeader;

    public MessageProcessor(RequestStore requestStore, StatsEngine statsEngine, Clock clock,
            List<MessageCheck<?, ?>> checks, CorrelationExtractor correlationExtractor, String correlationHeaderName,
            boolean useTimestampHeader) {
        this.requestStore = requestStore;
        this.statsEngine = statsEngine;
        this.clock = clock;
        this.checks = checks;
        this.correlationExtractor = correlationExtractor;
        this.correlationHeaderName = correlationHeaderName;
        this.useTimestampHeader = useTimestampHeader;
    }

    public void process(List<ConsumerRecord<String, Object>> records) {
        long defaultEndTime = clock.nowMillis();

        // 1. Extract correlation IDs and map them to records
        Map<String, Object> correlationIdToValue = new HashMap<>();
        for (ConsumerRecord<String, Object> record : records) {
            String correlationId = null;

            // Try to extract from headers first (default strategy)
            if (record.headers() != null) {
                for (org.apache.kafka.common.header.Header header : record.headers()) {
                    if (correlationHeaderName.equals(header.key())) {
                        correlationId = new String(header.value(), StandardCharsets.UTF_8);
                        break;
                    }
                }
            }

            // If not found in headers, try body extraction if configured
            if (correlationId == null && correlationExtractor != null && record.value() != null) {
                correlationId = correlationExtractor.extract(record);
            }

            if (correlationId != null) {
                // If using timestamp header, we need to pass the whole record to access the
                // timestamp later
                // Otherwise, we just pass the value as before (though passing record is safe
                // too as long as we handle it)
                if (useTimestampHeader) {
                    correlationIdToValue.put(correlationId, record);
                } else {
                    correlationIdToValue.put(correlationId, record.value());
                }
            }
        }

        // 2. Batch retrieve and remove requests from store
        requestStore.processBatchedRecords(correlationIdToValue, new BatchProcessor() {
            @Override
            public void onMatch(String correlationId, Map<String, Object> requestData, Object responseValue) {
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

                // Default to OK
                Status status = Status.apply("OK");
                Option<String> errorMessage = Option.empty();
                Option<String> responseCode = Option.apply("200"); // Simulate OK

                // Run checks if any
                if (checks != null && !checks.isEmpty()) {
                    for (MessageCheck<?, ?> check : checks) {
                        try {
                            // Deserialize request
                            Object request = null;
                            if (check.getRequestSerdeType() == io.github.kbdering.kafka.util.SerializationType.STRING) {
                                byte[] requestBytes = (byte[]) requestData.get(RequestStore.VALUE_BYTES);
                                if (requestBytes != null) {
                                    request = new String(requestBytes, StandardCharsets.UTF_8);
                                }
                            } else if (check
                                    .getRequestSerdeType() == io.github.kbdering.kafka.util.SerializationType.BYTE_ARRAY) {
                                request = (byte[]) requestData.get(RequestStore.VALUE_BYTES);
                            }

                            // Deserialize response
                            Object response = null;
                            if (check
                                    .getResponseSerdeType() == io.github.kbdering.kafka.util.SerializationType.STRING) {
                                if (actualResponseValue instanceof byte[]) {
                                    response = new String((byte[]) actualResponseValue, StandardCharsets.UTF_8);
                                } else {
                                    response = actualResponseValue.toString();
                                }
                            } else if (check
                                    .getResponseSerdeType() == io.github.kbdering.kafka.util.SerializationType.BYTE_ARRAY) {
                                if (actualResponseValue instanceof byte[]) {
                                    response = actualResponseValue;
                                } else {
                                    // Fallback or error? For now, toString bytes
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

            @Override
            public void onUnmatched(String correlationId, Object responseValue) {
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
                    // If we want to use the message timestamp as end time, we can.
                    // But for unmatched, we don't have start time, so duration is 0 or undefined.
                    // Let's keep it simple and use clock for unmatched or maybe use timestamp for
                    // both start and end?
                    // If we use timestamp for end, and clock for start, it might be weird.
                    // But here startTime is set to endTime (defaultEndTime).
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
    }
}
