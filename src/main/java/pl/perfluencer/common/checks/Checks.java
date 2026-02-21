/*
 * Copyright (c) 2025 Perfluencer. All rights reserved.
 * Contact: kuba@perfluencer.pl
 * 
 * This software is proprietary and confidential. Unauthorized copying,
 * modification, distribution, or use of this software, in whole or in part,
 * is strictly prohibited without the express written permission of Perfluencer.
 */
package pl.perfluencer.common.checks;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import com.jayway.jsonpath.JsonPath;

/**
 * Static utility class providing common message check shortcuts.
 * 
 * <h2>Usage:</h2>
 * Import statically and use directly in simulations:
 * 
 * <pre>{@code
 * import static pl.perfluencer.common.checks.Checks.*;
 * 
 * // Quick checks
 * .check(responseContains("SUCCESS"))
 * .check(jsonPathEquals("$.orderId", "ORD-123"))
 * .check(echoCheck())  // Request equals response
 * .check(fieldEquals("$.id", "$.responseId"))
 * }</pre>
 *
 * @author Jakub Dering
 */
public final class Checks {

    private Checks() {
        // Utility class
    }

    // ==================== QUICK CHECK FACTORIES ====================

    /**
     * Creates an echo check - verifies response equals request.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> echoCheck() {
        return MessageCheckBuilder.strings()
                .named("Echo Check")
                .verify(String::equals);
    }

    /**
     * Creates a check that response contains the request.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> responseContainsRequest() {
        return MessageCheckBuilder.strings()
                .named("Response Contains Request")
                .verify((req, res) -> res != null && res.contains(req));
    }

    /**
     * Creates a check that response contains a specific substring.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> responseContains(String substring) {
        return MessageCheckBuilder.strings()
                .named("Response Contains '" + substring + "'")
                .verifyResponse(res -> res != null && res.contains(substring));
    }

    /**
     * Creates a check that response matches a regex pattern.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> responseMatches(String regex) {
        return MessageCheckBuilder.strings()
                .named("Response Matches Pattern")
                .verifyResponse(res -> res != null && res.matches(regex));
    }

    /**
     * Creates a check that response is not null or empty.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> responseNotEmpty() {
        return MessageCheckBuilder.strings()
                .named("Response Not Empty")
                .verifyResponse(res -> res != null && !res.isEmpty());
    }

    /**
     * Creates a check that response equals expected value.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> responseEquals(String expected) {
        return MessageCheckBuilder.strings()
                .named("Response Equals")
                .verifyResponse(res -> Objects.equals(expected, res));
    }

    // ==================== JSON SHORTCUTS ====================

    /**
     * Checks that a JSONPath in the response equals an expected value.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> jsonPathEquals(
            String jsonPath, Object expectedValue) {
        return MessageCheckBuilder.json()
                .named("JSONPath " + jsonPath + " = " + expectedValue)
                .responsePathEquals(jsonPath, expectedValue);
    }

    /**
     * Checks that a JSONPath in the response is not null or empty.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> jsonPathNotEmpty(String jsonPath) {
        return MessageCheckBuilder.json()
                .named("JSONPath " + jsonPath + " not empty")
                .responsePathNotEmpty(jsonPath);
    }

    /**
     * Checks that a JSONPath exists in the response.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> jsonPathExists(String jsonPath) {
        return MessageCheckBuilder.json()
                .named("JSONPath " + jsonPath + " exists")
                .responseHasPath(jsonPath);
    }

    /**
     * Checks that JSONPath values in request and response match.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> jsonFieldEquals(
            String requestPath, String responsePath) {
        return MessageCheckBuilder.json()
                .named("Field Match: " + requestPath + " = " + responsePath)
                .pathEquals(requestPath, responsePath);
    }

    /**
     * Checks that response JSONPath matches a predicate.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> jsonPathMatches(
            String jsonPath, Predicate<Object> predicate) {
        return MessageCheckBuilder.json()
                .named("JSONPath " + jsonPath + " matches predicate")
                .responsePathMatches(jsonPath, Object.class, predicate);
    }

    /**
     * Checks that response JSONPath is greater than a value.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> jsonPathGreaterThan(
            String jsonPath, Number threshold) {
        return MessageCheckBuilder.strings()
                .named("JSONPath " + jsonPath + " > " + threshold)
                .check((req, res) -> {
                    try {
                        Number value = JsonPath.read(res, jsonPath);
                        if (value.doubleValue() > threshold.doubleValue()) {
                            return Optional.empty();
                        }
                        return Optional.of(jsonPath + " = " + value + " is not > " + threshold);
                    } catch (Exception e) {
                        return Optional.of("JSONPath error: " + e.getMessage());
                    }
                });
    }

    /**
     * Checks that response JSONPath is less than a value.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> jsonPathLessThan(
            String jsonPath, Number threshold) {
        return MessageCheckBuilder.strings()
                .named("JSONPath " + jsonPath + " < " + threshold)
                .check((req, res) -> {
                    try {
                        Number value = JsonPath.read(res, jsonPath);
                        if (value.doubleValue() < threshold.doubleValue()) {
                            return Optional.empty();
                        }
                        return Optional.of(jsonPath + " = " + value + " is not < " + threshold);
                    } catch (Exception e) {
                        return Optional.of("JSONPath error: " + e.getMessage());
                    }
                });
    }

    // ==================== STATUS CHECKS ====================

    /**
     * Checks for common success indicators in JSON responses.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> jsonSuccess() {
        return MessageCheckBuilder.strings()
                .named("JSON Success Status")
                .check((req, res) -> {
                    try {
                        // Try common success patterns
                        Object status = null;
                        try {
                            status = JsonPath.read(res, "$.status");
                        } catch (Exception ignored) {
                        }
                        if (status != null) {
                            String s = status.toString().toLowerCase();
                            if (s.equals("success") || s.equals("ok") || s.equals("true")) {
                                return Optional.empty();
                            }
                        }

                        // Try "success" boolean field
                        try {
                            Boolean success = JsonPath.read(res, "$.success");
                            if (Boolean.TRUE.equals(success)) {
                                return Optional.empty();
                            }
                        } catch (Exception ignored) {
                        }

                        // Try "error" field (should be absent or null)
                        try {
                            Object error = JsonPath.read(res, "$.error");
                            if (error == null) {
                                return Optional.empty();
                            }
                        } catch (Exception ignored) {
                            // No error field is good
                            return Optional.empty();
                        }

                        return Optional.of("No success indicator found in response");
                    } catch (Exception e) {
                        return Optional.of("Error checking success: " + e.getMessage());
                    }
                });
    }

    // ==================== GATLING-STYLE CHECK CHAIN ====================

    /**
     * Creates a regex extractor for fluent check chaining.
     *
     * <pre>{@code
     * regex("orderId=(\\w+)").find(1).is("ORD-123")
     * regex("item=\\w+").count().is(3)
     * }</pre>
     */
    public static RegexExtractor regex(String pattern) {
        return MessageCheckBuilder.regex(pattern);
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
        return MessageCheckBuilder.xpath(expression);
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
        return MessageCheckBuilder.substring(text);
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
        return MessageCheckBuilder.bodyString();
    }

    /**
     * Creates a JSONPath extractor for fluent check chaining.
     *
     * <pre>{@code
     * jsonPath("$.status").find().is("OK")
     * jsonPath("$.amount").ofInt().gt(100)
     * }</pre>
     */
    public static JsonPathExtractor jsonPath(String expression) {
        return MessageCheckBuilder.jsonPath(expression);
    }

    /**
     * Creates a typed field extractor for POJO/Protobuf getter chains.
     *
     * <pre>{@code
     * field(OrderResponse.class).get(OrderResponse::getStatus).is("OK")
     * }</pre>
     */
    public static <ResT> FieldExtractor<ResT> field(Class<ResT> responseClass) {
        return MessageCheckBuilder.field(responseClass);
    }

    // ==================== CUSTOM BUILDER ACCESS ====================

    /**
     * Access the full builder for custom checks.
     */
    public static <ReqT, ResT> MessageCheckBuilder<ReqT, ResT> custom(
            Class<ReqT> requestClass, Class<ResT> responseClass) {
        return MessageCheckBuilder.forTypes(requestClass, responseClass);
    }

    /**
     * Creates a custom predicate-based check.
     */
    public static MessageCheckBuilder.MessageCheckResult<String, String> when(
            String name, BiPredicate<String, String> predicate) {
        return MessageCheckBuilder.strings()
                .named(name)
                .verify(predicate);
    }
}
