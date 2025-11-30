package io.github.kbdering.kafka;

import java.util.Optional;
import java.util.function.BiFunction;
import io.github.kbdering.kafka.util.SerializationType;

/**
 * Defines a validation check for request-reply message patterns in Kafka load
 * tests.
 * 
 * <p>
 * A {@code MessageCheck} allows you to validate that the response message
 * received
 * from Kafka matches the expected content based on the original request. This
 * is crucial
 * for measuring Quality of Service (QoS) and ensuring data correctness under
 * load.
 * 
 * <h2>Type Parameters:</h2>
 * <ul>
 * <li>{@code ReqT} - The type of the request message (e.g., String, byte[],
 * custom POJO)</li>
 * <li>{@code ResT} - The type of the response message</li>
 * </ul>
 * 
 * <h2>Usage Example:</h2>
 * 
 * <pre>{@code
 * // Simple string equality check
 * MessageCheck<String, String> check = new MessageCheck<>(
 *         "Echo Check",
 *         String.class, SerializationType.STRING,
 *         String.class, SerializationType.STRING,
 *         (request, response) -> {
 *             if (request.equals(response)) {
 *                 return Optional.empty(); // Success
 *             }
 *             return Optional.of("Response doesn't match request");
 *         });
 * 
 * // JSON field validation
 * MessageCheck<String, String> jsonCheck = new MessageCheck<>(
 *         "JSON Status Check",
 *         String.class, SerializationType.STRING,
 *         String.class, SerializationType.STRING,
 *         (req, res) -> {
 *             JsonNode resJson = new ObjectMapper().readTree(res);
 *             if (resJson.path("status").asText().equals("success")) {
 *                 return Optional.empty();
 *             }
 *             return Optional.of("Status is not success");
 *         });
 * }</pre>
 * 
 * <h2>Validation Logic:</h2>
 * <p>
 * The {@code checkLogic} BiFunction should return:
 * <ul>
 * <li>{@code Optional.empty()} - validation passed</li>
 * <li>{@code Optional.of("error message")} - validation failed with reason</li>
 * </ul>
 * 
 * @param <ReqT> the request message type
 * @param <ResT> the response message type
 * @author Jakub Dering
 * @see SerializationType
 * @see io.github.kbdering.kafka.javaapi.KafkaDsl#kafkaRequestReply
 */
public class MessageCheck<ReqT, ResT> { // Now generic

    private final String checkName;
    private final Class<ReqT> requestClass;
    private final SerializationType requestSerdeType;
    private final Class<ResT> responseClass;
    private final SerializationType responseSerdeType; // Expected response serialization type
    private final BiFunction<ReqT, ResT, Optional<String>> checkLogic;

    /**
     * Creates a new message validation check.
     * 
     * @param checkName         a descriptive name for this check (used in error
     *                          messages)
     * @param requestClass      the class type of the request message
     * @param requestSerdeType  the serialization type used for the request
     * @param responseClass     the class type of the response message
     * @param responseSerdeType the serialization type expected for the response
     * @param checkLogic        the validation function that compares request and
     *                          response,
     *                          returning {@code Optional.empty()} on success or an
     *                          error message on failure
     */
    public MessageCheck(String checkName,
            Class<ReqT> requestClass, SerializationType requestSerdeType,
            Class<ResT> responseClass, SerializationType responseSerdeType,
            BiFunction<ReqT, ResT, Optional<String>> checkLogic) {
        this.checkName = checkName;
        this.requestClass = requestClass;
        this.requestSerdeType = requestSerdeType;
        this.responseClass = responseClass;
        this.responseSerdeType = responseSerdeType;
        this.checkLogic = checkLogic;
    }

    public String getCheckName() {
        return checkName;
    }

    public Class<ReqT> getRequestClass() {
        return requestClass;
    }

    public SerializationType getRequestSerdeType() {
        return requestSerdeType;
    }

    public Class<ResT> getResponseClass() {
        return responseClass;
    }

    public SerializationType getResponseSerdeType() {
        return responseSerdeType;
    }

    /**
     * Returns the validation logic function.
     * 
     * @return the BiFunction that performs the validation check
     */
    public BiFunction<ReqT, ResT, Optional<String>> getCheckLogic() {
        return checkLogic;
    }
}
