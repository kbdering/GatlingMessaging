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
import java.util.Map;

public class MessageProcessorActor extends AbstractActor {

    private final RequestStore requestStore;
    private final CoreComponents coreComponents;
    private final StatsEngine statsEngine;

    public MessageProcessorActor(RequestStore requestStore, CoreComponents coreComponents, List<MessageCheck> checks) {
        this.requestStore = requestStore;
        this.coreComponents = coreComponents;
        this.statsEngine = coreComponents.statsEngine(); // Cache for convenience
    }

    public static Props props(RequestStore requestStore, CoreComponents coreComponents, List<MessageCheck> checks) {
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
        ConsumerRecord<String, String> record = processMessage.record;
        long endTime = processMessage.consumeEndTime; // Use the time the batch was polled
        String correlationId = null;

        if (record.headers().lastHeader("correlationId") != null) {
            correlationId = new String(record.headers().lastHeader("correlationId").value());
        }

        if (correlationId != null) {
            long startTime = endTime; // Default start time if lookup fails early
            String transactionName = "Missing Match"; // Default transaction name

            try {
                // Potential blocking call - consider if requestStore needs its own dispatcher
                // Using SELECT FOR UPDATE here might require careful transaction management
                // if the goal is to lock until processing is complete.
                Map<String, String> requestData = requestStore.getRequest(record.key()); // Using record.key() - ensure this is correct ID

                if (requestData != null && !requestData.isEmpty()) {
                    startTime = Long.parseLong(requestData.get("startTime"));
                    transactionName = requestData.get("transactionName"); // Use looked-up name

                    // Log success
                    statsEngine.logResponse(
                            "kafka-processor", // Actor group name
                            List$.MODULE$.empty(),
                            transactionName,
                            startTime,
                            endTime, // Use the time the batch was polled
                            Status.apply("OK"),
                            Option.apply("200"), // Simulate HTTP status OK? Or keep custom?
                            Option.empty() // No message on OK
                    );

                    // Delete the request after successful processing
                    requestStore.deleteRequest(correlationId); // Ensure correct ID is used for deletion

                } else {
                    // Log request data not found
                    System.err.println("Request data not found for correlationId: " + correlationId + " (Key: " + record.key() + ")");
                    statsEngine.logResponse(
                            "kafka-processor",
                            List$.MODULE$.empty(),
                            transactionName, // "Missing Match"
                            startTime,
                            endTime,
                            Status.apply("KO"), // Indicate failure
                            Option.apply("404"), // Simulate Not Found?
                            Option.apply("Request data not found for correlationId: " + correlationId)
                    );
                }
            } catch (Exception e) {
                // Log processing error
                System.err.println("Error processing record for correlationId " + correlationId + " (Key: " + record.key() + "): " + e.getMessage());
                e.printStackTrace(); // Log stack trace for debugging
                statsEngine.logResponse(
                        "kafka-processor",
                        List$.MODULE$.empty(),
                        transactionName, // May still be "Missing Match" or the looked-up name if error occurred later
                        startTime,
                        endTime,
                        Status.apply("KO"), // Indicate failure
                        Option.apply("500"), // Simulate Internal Server Error?
                        Option.apply("Error processing: " + e.getMessage())
                );
            }
        } else {
            // Log message without correlation ID
            System.err.println("Received message without correlation ID (Key: " + record.key() + "). Ignoring.");
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