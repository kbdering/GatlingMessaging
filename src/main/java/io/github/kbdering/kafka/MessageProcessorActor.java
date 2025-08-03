package io.github.kbdering.kafka;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.gatling.commons.stats.Status;
import io.gatling.core.CoreComponents;
import io.gatling.core.stats.StatsEngine;
import io.github.kbdering.kafka.cache.BatchProcessor;
import io.github.kbdering.kafka.cache.RequestStore;


import scala.Option; // Use Option explicitly
import scala.collection.immutable.List$; // For empty list


import java.util.List;
import java.util.Optional;
import java.util.Map;

public class MessageProcessorActor extends AbstractActor {

    private final RequestStore requestStore;
    private final StatsEngine statsEngine;
    private final List<MessageCheck<?, ?>> checks; // Use wildcard for generic list
    private final CoreComponents coreComponents;



    public MessageProcessorActor(RequestStore requestStore, CoreComponents coreComponents, List<MessageCheck<?, ?>> checks) {
        this.requestStore = requestStore;
        this.statsEngine = coreComponents.statsEngine(); // Cache for convenience
        this.checks = checks;
        this.coreComponents = coreComponents;
    }

    public static Props props(RequestStore requestStore, CoreComponents coreComponents, List<MessageCheck<?, ?>> checks) {
        // Ensure dependencies are non-null if required
        java.util.Objects.requireNonNull(requestStore, "RequestStore cannot be null");
        java.util.Objects.requireNonNull(coreComponents, "CoreComponents cannot be null");
        return Props.create(MessageProcessorActor.class, () -> new MessageProcessorActor(requestStore, coreComponents, checks));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Map.class, this::process)
                .build();
    }

    private void process(Map<String, byte[]> records) {

        long endTime = coreComponents.clock().nowMillis();


        requestStore.processBatchedRecords(records,  new BatchProcessor() {
            @Override
            public void onMatch(String correlationId, Map<String, Object> requestData, byte[] responseBytes) {
                String transactionName;
                Long startTime;
                Status status;
                Option<String> errorMessage;
                Option<String> responseCode;

                

                // Update start time and transaction name from stored data
                startTime = Long.parseLong((String) requestData.get(RequestStore.START_TIME));
                transactionName = (String) requestData.get(RequestStore.TRANSACTION_NAME); // Use looked-up name

                byte[] storedRequestBytes = (byte[]) requestData.get(RequestStore.VALUE_BYTES);
                SerializationType storedRequestSerdeType = (SerializationType) requestData.get(RequestStore.SERIALIZATION_TYPE);

                Optional<String> checkFailure = Optional.empty();
                if (checks != null) { // Ensure checks list is not null
                     for (MessageCheck<?, ?> untypedCheck : checks) {
                            // Cast to specific types based on the check's metadata
                            MessageCheck<Object, Object> currentCheck = (MessageCheck<Object, Object>) untypedCheck;

                            Object deserializedRequest = SerializationHelper.deserialize(
                                    storedRequestBytes,
                                    storedRequestSerdeType, // Use the type stored with the request
                                    currentCheck.getRequestClass()
                            );

                            // For response, we use the type defined in the MessageCheck
                            Object deserializedResponse = SerializationHelper.deserialize(
                                    responseBytes,
                                    currentCheck.getResponseSerdeType(), // Expected response type from check
                                    currentCheck.getResponseClass()
                            );

                            Optional<String> result = currentCheck.getCheckLogic().apply(deserializedRequest, deserializedResponse);

                            if (result.isPresent()) { // Check failed
                                checkFailure = Optional.of("Check '" + untypedCheck.getCheckName() + "' failed: " + result.get());
                                break; // Stop on first failure
                            }
                        }
                    }

                    if (checkFailure.isPresent()) {
                        // Validation failed
                        status = Status.apply("KO");
                        errorMessage = Option.apply(checkFailure.get());
                        responseCode = Option.apply("400"); // Simulate Bad Request due to validation
                        //System.err.println("Validation failed for correlationId " + correlationId + ": " + checkFailure.get());
                    } else {
                        // All checks passed
                        status = Status.apply("OK");
                        errorMessage = Option.empty();
                        responseCode = Option.apply("200"); // Simulate OK
                    }
                
                        statsEngine.logResponse(
                        "kafka-processor",
                        List$.MODULE$.empty(),
                        transactionName, // May still be "Missing Match" or the looked-up name if error occurred later
                        startTime,
                        endTime,
                        status,
                        responseCode,
                        errorMessage
                    );

        }
        @Override
            public void onUnmatched(String correlationId, byte[] responseBytes) {
            long startTime = endTime; // Default start time if lookup fails early
            String transactionName = "Missing Match"; // Default transaction name
            Status status = Status.apply("KO"); // Default status is failure
            Option<String> errorMessage = Option.apply("Request data not found for correlationId");
            Option<String> responseCode = Option.apply("404"); // Simulate Not Found
                            statsEngine.logResponse(
                        "kafka-processor",
                        List$.MODULE$.empty(),
                        transactionName, // May still be "Missing Match" or the looked-up name if error occurred later
                        startTime,
                        endTime,
                        status,
                        responseCode,
                        errorMessage
                    );

            
    }
});
    }   
};



