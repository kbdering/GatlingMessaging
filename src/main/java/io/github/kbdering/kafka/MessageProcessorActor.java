package io.github.kbdering.kafka;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.gatling.commons.stats.Status;
import io.gatling.core.CoreComponents;
import io.gatling.core.stats.StatsEngine;
import io.github.kbdering.kafka.KafkaMessages.ProcessRecord; // Import the message
import io.github.kbdering.kafka.cache.RequestStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Option; // Use Option explicitly
import scala.collection.immutable.List$; // For empty list


import java.util.List;
import java.util.Optional;
import java.util.Map;

public class MessageProcessorActor extends AbstractActor {

    private final RequestStore requestStore;
    private final CoreComponents coreComponents;
    private final StatsEngine statsEngine;
    private final List<MessageCheck<?, ?>> checks; // Use wildcard for generic list


    public MessageProcessorActor(RequestStore requestStore, CoreComponents coreComponents, List<MessageCheck<?, ?>> checks) {
        this.requestStore = requestStore;
        this.coreComponents = coreComponents;
        this.statsEngine = coreComponents.statsEngine(); // Cache for convenience
        this.checks = checks;
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
                .match(ProcessRecord.class, this::process)
                .build();
    }

    private void process(ProcessRecord processMessage) {
        ConsumerRecord<String, byte[]> responseRecord = processMessage.record; // Now byte[]
        long endTime = processMessage.consumeEndTime; // Use the time the batch was polled
        String correlationId = null;

        // Extract correlationId from response headers
        if (responseRecord.headers().lastHeader("correlationId") != null) {
            correlationId = new String(responseRecord.headers().lastHeader("correlationId").value());
        }

        if (correlationId != null) {
            long startTime = endTime; // Default start time if lookup fails early
            String transactionName = "Missing Match"; // Default transaction name
            Status status = Status.apply("KO"); // Default status is failure
            Option<String> errorMessage = Option.apply("Request data not found for correlationId: " + correlationId);
            Option<String> responseCode = Option.apply("404"); // Simulate Not Found

            try {
                // Retrieve original request data using the correlation ID
                Map<String, Object> requestData = requestStore.getRequest(correlationId);

                if (requestData != null && !requestData.isEmpty()) {
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
                                    responseRecord.value(),
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
                        System.err.println("Validation failed for correlationId " + correlationId + ": " + checkFailure.get());
                    } else {
                        // All checks passed
                        status = Status.apply("OK");
                        errorMessage = Option.empty();
                        responseCode = Option.apply("200"); // Simulate OK
                    }

                    // Delete the request after processing (regardless of validation outcome)
                    requestStore.deleteRequest(correlationId);

                } else {
                    // Log request data not found
                    // Use default status, error message, and response code defined above
                    System.err.println("Request data not found for correlationId: " + correlationId + " (Response Key: " + responseRecord.key() + ")");
                }
            } catch (Exception e) {
                // Log processing error
                System.err.println("Error processing record for correlationId " + correlationId + " (Response Key: " + responseRecord.key() + "): " + e.getMessage());
                e.printStackTrace(); // Log stack trace for debugging
                status = Status.apply("KO");
                errorMessage = Option.apply("Processing error: " + e.getMessage());
                responseCode = Option.apply("500"); // Simulate Internal Server Error
            } finally {
                // Log the response status based on the outcome
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
        } else {
            // Log message without correlation ID
            System.err.println("Received message without correlation ID (Response Key: " + responseRecord.key() + "). Ignoring.");
            statsEngine.logResponse(
                    "kafka-processor",
                    List$.MODULE$.empty(),
                    "No CorrelationId", // Specific transaction name for this case
                    endTime, // No meaningful start time
                    endTime,
                    Status.apply("KO"),
                    Option.apply("400"), // Simulate Bad Request?
                    Option.apply("Message received without correlationId header")
            );
        }
    }
}