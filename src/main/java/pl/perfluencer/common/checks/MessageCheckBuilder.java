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
package pl.perfluencer.common.checks;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import com.jayway.jsonpath.JsonPath;

/**
 * Fluent builder for creating message validation checks with type inference.
 * 
 * <h2>Usage Examples:</h2>
 * 
 * <pre>{@code
 * // Simple response-only check
 * var check = MessageCheckBuilder.forResponse(String.class)
 *         .named("Status Check")
 *         .verify(response -> response.contains("SUCCESS"));
 * 
 * // Request-response comparison
 * var echoCheck = MessageCheckBuilder.forTypes(String.class, String.class)
 *         .named("Echo Check")
 *         .verify((request, response) -> request.equals(response));
 * 
 * // JSONPath shortcuts
 * var jsonCheck = MessageCheckBuilder.json()
 *         .named("Order ID Match")
 *         .responsePathEquals("$.orderId", "ORD-123");
 * 
 * // Protobuf with field extraction
 * var protoCheck = MessageCheckBuilder.forTypes(OrderRequest.class, OrderResponse.class)
 *         .named("Order Confirmation")
 *         .verifyField(OrderRequest::getOrderId, OrderResponse::getOrderIdEcho, Objects::equals);
 * }</pre>
 *
 * @param <ReqT> the request message type
 * @param <ResT> the response message type
 * @author Jakub Dering
 */
public class MessageCheckBuilder<ReqT, ResT> {

    private final Class<ReqT> requestClass;
    private final Class<ResT> responseClass;
    private String checkName = "Unnamed Check";

    private MessageCheckBuilder(Class<ReqT> requestClass, Class<ResT> responseClass) {
        this.requestClass = requestClass;
        this.responseClass = responseClass;
    }

    // ==================== STATIC FACTORY METHODS ====================

    /**
     * Creates a builder for request-response checks with explicit types.
     */
    public static <ReqT, ResT> MessageCheckBuilder<ReqT, ResT> forTypes(
            Class<ReqT> requestClass, Class<ResT> responseClass) {
        return new MessageCheckBuilder<>(requestClass, responseClass);
    }

    /**
     * Creates a builder for response-only checks (request type is Void).
     */
    public static <ResT> MessageCheckBuilder<Void, ResT> forResponse(Class<ResT> responseClass) {
        return new MessageCheckBuilder<>(Void.class, responseClass);
    }

    /**
     * Creates a builder for String JSON checks.
     */
    public static JsonCheckBuilder json() {
        return new JsonCheckBuilder();
    }

    /**
     * Creates a builder for byte array checks.
     */
    public static MessageCheckBuilder<byte[], byte[]> bytes() {
        return new MessageCheckBuilder<>(byte[].class, byte[].class);
    }

    /**
     * Creates a builder for String checks.
     */
    public static MessageCheckBuilder<String, String> strings() {
        return new MessageCheckBuilder<>(String.class, String.class);
    }

    // ==================== GATLING-STYLE CHECK CHAIN ENTRY POINTS
    // ====================

    /**
     * Creates a regex extractor for fluent check chaining.
     *
     * <pre>{@code
     * regex("orderId=(\\w+)").find(1).is("ORD-123")
     * regex("item=\\w+").count().is(3)
     * }</pre>
     */
    public static RegexExtractor regex(String pattern) {
        return new RegexExtractor(pattern);
    }

    /**
     * Creates an XPath extractor for fluent check chaining.
     *
     * <pre>{@code
     * xpath("/response/status").find().is("OK")
     * xpath("/response/items/item").count().gt(0)
     * }</pre>
     */
    public static XPathExtractor xpath(String expression) {
        return new XPathExtractor(expression);
    }

    /**
     * Creates a substring extractor for fluent check chaining.
     *
     * <pre>{@code
     * substring("SUCCESS").find().exists()
     * substring("ERROR").count().is(0)
     * }</pre>
     */
    public static SubstringExtractor substring(String text) {
        return new SubstringExtractor(text);
    }

    /**
     * Creates a body string check step for the full response body.
     *
     * <pre>{@code
     * bodyString().is("exact match")
     * bodyString().transform(String::length).gt(10)
     * }</pre>
     */
    public static CheckStep<String> bodyString() {
        return new CheckStep<>("bodyString", response -> response);
    }

    /**
     * Creates a JSONPath extractor for fluent check chaining.
     *
     * <pre>{@code
     * jsonPath("$.status").find().is("OK")
     * jsonPath("$.amount").ofInt().gt(100)
     * jsonPath("$.items[*]").count().gt(0)
     * }</pre>
     */
    public static JsonPathExtractor jsonPath(String expression) {
        return new JsonPathExtractor(expression);
    }

    /**
     * Creates a typed field extractor for POJO/Protobuf getter chains.
     *
     * <pre>{@code
     * field(OrderResponse.class).get(OrderResponse::getStatus).is("OK")
     * field(OrderResponse.class).get(OrderResponse::getAmount).gt(100.0)
     * }</pre>
     *
     * @param responseClass the response class type
     * @param <ResT>        the response type
     */
    public static <ResT> FieldExtractor<ResT> field(Class<ResT> responseClass) {
        return new FieldExtractor<>(responseClass);
    }

    // ==================== BUILDER METHODS ====================

    /**
     * Sets the descriptive name for this check.
     */
    public MessageCheckBuilder<ReqT, ResT> named(String name) {
        this.checkName = name;
        return this;
    }

    // ==================== TERMINAL METHODS (BUILD CHECK) ====================

    /**
     * Creates a check that validates both request and response.
     * 
     * @param predicate returns true if check passes
     */
    public MessageCheckResult<ReqT, ResT> verify(BiPredicate<ReqT, ResT> predicate) {
        return new MessageCheckResult<>(checkName, requestClass, responseClass,
                (req, res) -> predicate.test(req, res) ? Optional.empty()
                        : Optional.of(checkName + " failed"));
    }

    /**
     * Creates a check with custom error message on failure.
     */
    public MessageCheckResult<ReqT, ResT> verify(BiPredicate<ReqT, ResT> predicate, String errorMessage) {
        return new MessageCheckResult<>(checkName, requestClass, responseClass,
                (req, res) -> predicate.test(req, res) ? Optional.empty()
                        : Optional.of(errorMessage));
    }

    /**
     * Creates a check with full control over validation logic.
     */
    public MessageCheckResult<ReqT, ResT> check(BiFunction<ReqT, ResT, Optional<String>> logic) {
        return new MessageCheckResult<>(checkName, requestClass, responseClass, logic);
    }

    /**
     * Creates a response-only check (ignores request).
     */
    public MessageCheckResult<ReqT, ResT> verifyResponse(Predicate<ResT> predicate) {
        return new MessageCheckResult<>(checkName, requestClass, responseClass,
                (req, res) -> predicate.test(res) ? Optional.empty()
                        : Optional.of(checkName + " failed"));
    }

    /**
     * Creates a response-only check with custom error message.
     */
    public MessageCheckResult<ReqT, ResT> verifyResponse(Predicate<ResT> predicate, String errorMessage) {
        return new MessageCheckResult<>(checkName, requestClass, responseClass,
                (req, res) -> predicate.test(res) ? Optional.empty()
                        : Optional.of(errorMessage));
    }

    /**
     * Extracts fields from request and response, then compares them.
     */
    public <F> MessageCheckResult<ReqT, ResT> verifyField(
            Function<ReqT, F> requestExtractor,
            Function<ResT, F> responseExtractor,
            BiPredicate<F, F> comparator) {
        return new MessageCheckResult<>(checkName, requestClass, responseClass,
                (req, res) -> {
                    F reqField = requestExtractor.apply(req);
                    F resField = responseExtractor.apply(res);
                    if (comparator.test(reqField, resField)) {
                        return Optional.empty();
                    }
                    return Optional.of(checkName + ": expected '" + reqField + "' but got '" + resField + "'");
                });
    }

    /**
     * Verifies that extracted fields are equal.
     */
    public <F> MessageCheckResult<ReqT, ResT> verifyFieldEquals(
            Function<ReqT, F> requestExtractor,
            Function<ResT, F> responseExtractor) {
        return verifyField(requestExtractor, responseExtractor,
                (a, b) -> a == null ? b == null : a.equals(b));
    }

    // ==================== JSON CHECK BUILDER ====================

    /**
     * Specialized builder for JSON string checks using JSONPath.
     */
    public static class JsonCheckBuilder {
        private String checkName = "JSON Check";

        public JsonCheckBuilder named(String name) {
            this.checkName = name;
            return this;
        }

        /**
         * Checks that a JSONPath in the response equals an expected value.
         */
        public MessageCheckResult<String, String> responsePathEquals(String jsonPath, Object expectedValue) {
            return new MessageCheckResult<>(checkName, String.class, String.class,
                    (req, res) -> {
                        try {
                            Object actual = JsonPath.read(res, jsonPath);
                            if (expectedValue.equals(actual)) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": " + jsonPath + " expected '"
                                    + expectedValue + "' but got '" + actual + "'");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": JSONPath error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Checks that a JSONPath in the response matches a predicate.
         */
        public <T> MessageCheckResult<String, String> responsePathMatches(
                String jsonPath, Class<T> type, Predicate<T> predicate) {
            return new MessageCheckResult<>(checkName, String.class, String.class,
                    (req, res) -> {
                        try {
                            T actual = JsonPath.read(res, jsonPath);
                            if (predicate.test(actual)) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": " + jsonPath + " = '" + actual + "' did not match");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": JSONPath error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Checks that request and response JSONPath values match.
         */
        public MessageCheckResult<String, String> pathEquals(String requestPath, String responsePath) {
            return new MessageCheckResult<>(checkName, String.class, String.class,
                    (req, res) -> {
                        try {
                            Object reqValue = JsonPath.read(req, requestPath);
                            Object resValue = JsonPath.read(res, responsePath);
                            if (reqValue == null && resValue == null) {
                                return Optional.empty();
                            }
                            if (reqValue != null && reqValue.equals(resValue)) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": " + requestPath + "='" + reqValue
                                    + "' != " + responsePath + "='" + resValue + "'");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": JSONPath error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Checks that the response contains a specific JSONPath.
         */
        public MessageCheckResult<String, String> responseHasPath(String jsonPath) {
            return new MessageCheckResult<>(checkName, String.class, String.class,
                    (req, res) -> {
                        try {
                            Object value = JsonPath.read(res, jsonPath);
                            if (value != null) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": path '" + jsonPath + "' not found");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": path '" + jsonPath + "' not found");
                        }
                    });
        }

        /**
         * Checks that a response JSONPath value is not null or empty.
         */
        public MessageCheckResult<String, String> responsePathNotEmpty(String jsonPath) {
            return new MessageCheckResult<>(checkName, String.class, String.class,
                    (req, res) -> {
                        try {
                            Object value = JsonPath.read(res, jsonPath);
                            if (value != null && !value.toString().isEmpty()) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": " + jsonPath + " is null or empty");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": " + jsonPath + " is null or empty");
                        }
                    });
        }
    }

    // ==================== RESULT TYPE ====================

    /**
     * Immutable result of building a message check.
     */
    public static class MessageCheckResult<ReqT, ResT> {
        private final String checkName;
        private final Class<ReqT> requestClass;
        private final Class<ResT> responseClass;
        private final BiFunction<ReqT, ResT, Optional<String>> checkLogic;

        public MessageCheckResult(String checkName, Class<ReqT> requestClass, Class<ResT> responseClass,
                BiFunction<ReqT, ResT, Optional<String>> checkLogic) {
            this.checkName = checkName;
            this.requestClass = requestClass;
            this.responseClass = responseClass;
            this.checkLogic = checkLogic;
        }

        public String getCheckName() {
            return checkName;
        }

        public Class<ReqT> getRequestClass() {
            return requestClass;
        }

        public Class<ResT> getResponseClass() {
            return responseClass;
        }

        public BiFunction<ReqT, ResT, Optional<String>> getCheckLogic() {
            return checkLogic;
        }

        /**
         * Validates the response against the request.
         */
        @SuppressWarnings("unchecked")
        public Optional<String> validate(Object request, Object response) {
            try {
                ReqT typedRequest = (ReqT) request;
                ResT typedResponse = (ResT) response;
                return checkLogic.apply(typedRequest, typedResponse);
            } catch (ClassCastException e) {
                return Optional.of("Type mismatch in " + checkName + ": " + e.getMessage());
            } catch (Exception e) {
                return Optional.of("Check error in " + checkName + ": " + e.getMessage());
            }
        }
    }
}
