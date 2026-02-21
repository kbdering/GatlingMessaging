package pl.perfluencer.kafka;

import java.util.Optional;
import java.util.function.BiFunction;
import pl.perfluencer.common.checks.JsonPathExtractor;
import pl.perfluencer.common.checks.RegexExtractor;
import pl.perfluencer.common.checks.SubstringExtractor;
import pl.perfluencer.common.checks.XPathExtractor;
import pl.perfluencer.common.util.SerializationType;

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
 * @see pl.perfluencer.kafka.javaapi.KafkaDsl#kafkaRequestReply
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

    // ==================== BUILDER INTEGRATION ====================

    /**
     * Creates a MessageCheck from a MessageCheckBuilder result.
     * Uses STRING serialization by default for both request and response.
     * 
     * @param result the builder result
     * @return a new MessageCheck
     */
    public static <ReqT, ResT> MessageCheck<ReqT, ResT> from(
            pl.perfluencer.common.checks.MessageCheckBuilder.MessageCheckResult<ReqT, ResT> result) {
        return from(result, SerializationType.STRING, SerializationType.STRING);
    }

    /**
     * Creates a MessageCheck from a MessageCheckBuilder result with explicit
     * serialization types.
     * 
     * @param result            the builder result
     * @param requestSerdeType  serialization type for request
     * @param responseSerdeType serialization type for response
     * @return a new MessageCheck
     */
    public static <ReqT, ResT> MessageCheck<ReqT, ResT> from(
            pl.perfluencer.common.checks.MessageCheckBuilder.MessageCheckResult<ReqT, ResT> result,
            SerializationType requestSerdeType,
            SerializationType responseSerdeType) {
        return new MessageCheck<>(
                result.getCheckName(),
                result.getRequestClass(),
                requestSerdeType,
                result.getResponseClass(),
                responseSerdeType,
                result.getCheckLogic());
    }

    /**
     * Creates a simple string echo check.
     */
    public static MessageCheck<String, String> echoCheck() {
        return from(pl.perfluencer.common.checks.Checks.echoCheck());
    }

    /**
     * Creates a check that verifies response contains a substring.
     */
    public static MessageCheck<String, String> responseContains(String substring) {
        return from(pl.perfluencer.common.checks.Checks.responseContains(substring));
    }

    /**
     * Creates a check that verifies a JSONPath equals expected value.
     */
    public static MessageCheck<String, String> jsonPathEquals(String jsonPath, Object expectedValue) {
        return from(pl.perfluencer.common.checks.Checks.jsonPathEquals(jsonPath, expectedValue));
    }

    /**
     * Creates a check that verifies JSONPath values in request and response match.
     */
    public static MessageCheck<String, String> jsonFieldEquals(String requestPath, String responsePath) {
        return from(pl.perfluencer.common.checks.Checks.jsonFieldEquals(requestPath, responsePath));
    }

    /**
     * Creates a check that verifies response is not empty.
     */
    public static MessageCheck<String, String> responseNotEmpty() {
        return from(pl.perfluencer.common.checks.Checks.responseNotEmpty());
    }

    // ==================== GATLING-STYLE CHECK CHAIN ====================

    /**
     * Creates a regex extractor for fluent check chaining.
     *
     * <pre>{@code
     * .check(MessageCheck.regex("orderId=(\\w+)").find(1).is("ORD-123"))
     * }</pre>
     */
    public static RegexExtractor regex(String pattern) {
        return pl.perfluencer.common.checks.MessageCheckBuilder.regex(pattern);
    }

    /**
     * Creates an XPath extractor for fluent check chaining.
     *
     * <pre>{@code
     * .check(MessageCheck.xpath("/response/status").find().is("OK"))
     * }</pre>
     */
    public static XPathExtractor xpath(String expression) {
        return pl.perfluencer.common.checks.MessageCheckBuilder.xpath(expression);
    }

    /**
     * Creates a substring extractor for fluent check chaining.
     *
     * <pre>{@code
     * .check(MessageCheck.substring("SUCCESS").find().exists())
     * }</pre>
     */
    public static SubstringExtractor substring(String text) {
        return pl.perfluencer.common.checks.MessageCheckBuilder.substring(text);
    }

    /**
     * Creates a JSONPath extractor for fluent check chaining.
     *
     * <pre>{@code
     * .check(MessageCheck.jsonPath("$.status").find().is("OK"))
     * }</pre>
     */
    public static JsonPathExtractor jsonPath(String expression) {
        return pl.perfluencer.common.checks.MessageCheckBuilder.jsonPath(expression);
    }
}
