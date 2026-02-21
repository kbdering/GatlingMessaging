/*
 * Copyright 2026 Perfluencer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.perfluencer.kafka;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import pl.perfluencer.common.util.SerializationType;

/**
 * Defines a session-aware validation check for request-reply message patterns
 * in Kafka load tests.
 * 
 * <p>
 * Unlike {@link MessageCheck} which validates response against the original
 * request payload,
 * {@code SessionAwareMessageCheck} validates the response against user-defined
 * session variables
 * that were persisted at the time of the request. This is ideal for scenarios
 * with large payloads
 * where storing session variables (like accountId, amount, correlationId) is
 * more memory-efficient.
 * 
 * <h2>Type Parameters:</h2>
 * <ul>
 * <li>{@code ResT} - The type of the response message (e.g., String, byte[],
 * custom POJO)</li>
 * </ul>
 * 
 * <h2>Usage Example:</h2>
 * 
 * <pre>{@code
 * // Validate response contains the accountId from session
 * SessionAwareMessageCheck<String> accountCheck = new SessionAwareMessageCheck<>(
 *         "Account ID Check",
 *         String.class, SerializationType.STRING,
 *         (sessionVars, response) -> {
 *             String expectedAccountId = sessionVars.get("accountId");
 *             if (response.contains("\"accountId\":\"" + expectedAccountId + "\"")) {
 *                 return Optional.empty(); // Success
 *             }
 *             return Optional.of("Account ID mismatch: expected " + expectedAccountId);
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
 * @param <ResT> the response message type
 * @author Jakub Dering
 * @see MessageCheck
 * @see pl.perfluencer.kafka.javaapi.KafkaDsl#kafkaRequestReply
 */
public class SessionAwareMessageCheck<ResT> extends MessageCheck<Void, ResT> {

    private final String checkName;
    private final Class<ResT> responseClass;
    private final SerializationType responseSerdeType;
    private final BiFunction<Map<String, String>, ResT, Optional<String>> checkLogic;

    /**
     * Creates a new session-aware message validation check.
     * 
     * @param checkName         a descriptive name for this check (used in error
     *                          messages)
     * @param responseClass     the class type of the response message
     * @param responseSerdeType the serialization type expected for the response
     * @param checkLogic        the validation function that compares session
     *                          variables and response,
     *                          returning {@code Optional.empty()} on success or an
     *                          error message on failure
     */
    public SessionAwareMessageCheck(String checkName,
            Class<ResT> responseClass, SerializationType responseSerdeType,
            BiFunction<Map<String, String>, ResT, Optional<String>> checkLogic) {
        super(checkName, Void.class, SerializationType.STRING, responseClass, responseSerdeType,
                (req, res) -> Optional.empty());
        this.checkName = checkName;
        this.responseClass = responseClass;
        this.responseSerdeType = responseSerdeType;
        this.checkLogic = checkLogic;
    }

    public String getCheckName() {
        return checkName;
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
    public BiFunction<Map<String, String>, ResT, Optional<String>> getSessionCheckLogic() {
        return checkLogic;
    }

    /**
     * Validates the response against session variables.
     * 
     * @param sessionVariables the session variables stored at request time
     * @param response         the response received
     * @return Optional.empty() if validation passes, or error message if it fails
     */
    @SuppressWarnings("unchecked")
    public Optional<String> validate(Map<String, String> sessionVariables, Object response) {
        try {
            ResT typedResponse = (ResT) response;
            return checkLogic.apply(sessionVariables, typedResponse);
        } catch (ClassCastException e) {
            return Optional.of("Type mismatch: " + e.getMessage());
        }
    }
}
