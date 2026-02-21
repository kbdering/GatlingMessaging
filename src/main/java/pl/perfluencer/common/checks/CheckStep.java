/*
 * Copyright (c) 2025 Perfluencer. All rights reserved.
 * Contact: kuba@perfluencer.pl
 * 
 * This software is proprietary and confidential. Unauthorized copying,
 * modification, distribution, or use of this software, in whole or in part,
 * is strictly prohibited without the express written permission of Perfluencer.
 */
package pl.perfluencer.common.checks;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Generic fluent validator step in the Gatling-style check chain.
 * 
 * <p>
 * Represents an extracted value ready for validation. Supports chaining
 * with transforms and multiple validator terminals.
 * 
 * <h2>Usage:</h2>
 * 
 * <pre>{@code
 * // Direct validation
 * regex("status=(\\w+)").find(1).is("OK")
 * 
 * // Transform then validate
 * regex("amount=(\\d+)").find(1).transform(Integer::parseInt).gt(100)
 * 
 * // Existence check
 * xpath("/response/id").find().exists()
 * }</pre>
 *
 * @param <T> the type of the extracted value
 * @author Jakub Dering
 */
public class CheckStep<T> {

    private final String checkName;
    private final Function<String, T> extractor;

    /**
     * Creates a new check step.
     *
     * @param checkName descriptive name for error messages
     * @param extractor function that extracts value from the response string
     */
    public CheckStep(String checkName, Function<String, T> extractor) {
        this.checkName = checkName;
        this.extractor = extractor;
    }

    // ==================== TRANSFORM ====================

    /**
     * Transforms the extracted value before validation.
     *
     * @param fn  transformation function
     * @param <U> the transformed type
     * @return a new CheckStep with the transformation applied
     */
    public <U> CheckStep<U> transform(Function<T, U> fn) {
        return new CheckStep<>(checkName, response -> fn.apply(extractor.apply(response)));
    }

    // ==================== TERMINAL VALIDATORS ====================

    /**
     * Validates that the extracted value equals the expected value.
     */
    public MessageCheckBuilder.MessageCheckResult<String, String> is(T expected) {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (expected.equals(actual)) {
                return Optional.empty();
            }
            return Optional.of(checkName + ": expected '" + expected + "' but got '" + actual + "'");
        });
    }

    /**
     * Validates that the extracted value does NOT equal the unexpected value.
     */
    public MessageCheckBuilder.MessageCheckResult<String, String> not(T unexpected) {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (!unexpected.equals(actual)) {
                return Optional.empty();
            }
            return Optional.of(checkName + ": should not be '" + unexpected + "'");
        });
    }

    /**
     * Validates that the extracted value is not null.
     */
    public MessageCheckBuilder.MessageCheckResult<String, String> exists() {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (actual != null) {
                return Optional.empty();
            }
            return Optional.of(checkName + ": value not found");
        });
    }

    /**
     * Validates that the extracted value is null (not found).
     */
    public MessageCheckBuilder.MessageCheckResult<String, String> notExists() {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (actual == null) {
                return Optional.empty();
            }
            return Optional.of(checkName + ": expected no value but found '" + actual + "'");
        });
    }

    /**
     * Validates that the extracted value is one of the given values.
     */
    @SafeVarargs
    public final MessageCheckBuilder.MessageCheckResult<String, String> in(T... values) {
        List<T> allowed = Arrays.asList(values);
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (allowed.contains(actual)) {
                return Optional.empty();
            }
            return Optional.of(checkName + ": '" + actual + "' not in " + allowed);
        });
    }

    /**
     * Validates that the extracted value satisfies a custom predicate.
     */
    public MessageCheckBuilder.MessageCheckResult<String, String> satisfies(Predicate<T> predicate) {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (actual != null && predicate.test(actual)) {
                return Optional.empty();
            }
            return Optional.of(checkName + ": value '" + actual + "' did not satisfy predicate");
        });
    }

    /**
     * Validates that the extracted Comparable value is greater than the threshold.
     */
    @SuppressWarnings("unchecked")
    public MessageCheckBuilder.MessageCheckResult<String, String> gt(Comparable<?> threshold) {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (actual == null) {
                return Optional.of(checkName + ": value is null, cannot compare");
            }
            try {
                Comparable<Object> comparableActual = (Comparable<Object>) actual;
                if (comparableActual.compareTo(threshold) > 0) {
                    return Optional.empty();
                }
                return Optional.of(checkName + ": " + actual + " is not > " + threshold);
            } catch (ClassCastException e) {
                return Optional.of(checkName + ": cannot compare " + actual + " with " + threshold);
            }
        });
    }

    /**
     * Validates that the extracted Comparable value is less than the threshold.
     */
    @SuppressWarnings("unchecked")
    public MessageCheckBuilder.MessageCheckResult<String, String> lt(Comparable<?> threshold) {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (actual == null) {
                return Optional.of(checkName + ": value is null, cannot compare");
            }
            try {
                Comparable<Object> comparableActual = (Comparable<Object>) actual;
                if (comparableActual.compareTo(threshold) < 0) {
                    return Optional.empty();
                }
                return Optional.of(checkName + ": " + actual + " is not < " + threshold);
            } catch (ClassCastException e) {
                return Optional.of(checkName + ": cannot compare " + actual + " with " + threshold);
            }
        });
    }

    /**
     * Validates that the extracted Comparable value is greater than or equal to the
     * threshold.
     */
    @SuppressWarnings("unchecked")
    public MessageCheckBuilder.MessageCheckResult<String, String> gte(Comparable<?> threshold) {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (actual == null) {
                return Optional.of(checkName + ": value is null, cannot compare");
            }
            try {
                Comparable<Object> comparableActual = (Comparable<Object>) actual;
                if (comparableActual.compareTo(threshold) >= 0) {
                    return Optional.empty();
                }
                return Optional.of(checkName + ": " + actual + " is not >= " + threshold);
            } catch (ClassCastException e) {
                return Optional.of(checkName + ": cannot compare " + actual + " with " + threshold);
            }
        });
    }

    /**
     * Validates that the extracted Comparable value is less than or equal to the
     * threshold.
     */
    @SuppressWarnings("unchecked")
    public MessageCheckBuilder.MessageCheckResult<String, String> lte(Comparable<?> threshold) {
        return buildResult((req, res) -> {
            T actual = extractor.apply(res);
            if (actual == null) {
                return Optional.of(checkName + ": value is null, cannot compare");
            }
            try {
                Comparable<Object> comparableActual = (Comparable<Object>) actual;
                if (comparableActual.compareTo(threshold) <= 0) {
                    return Optional.empty();
                }
                return Optional.of(checkName + ": " + actual + " is not <= " + threshold);
            } catch (ClassCastException e) {
                return Optional.of(checkName + ": cannot compare " + actual + " with " + threshold);
            }
        });
    }

    // ==================== INTERNAL ====================

    private MessageCheckBuilder.MessageCheckResult<String, String> buildResult(
            java.util.function.BiFunction<String, String, Optional<String>> logic) {
        return new MessageCheckBuilder.MessageCheckResult<>(
                checkName, String.class, String.class,
                (req, res) -> {
                    try {
                        return logic.apply(req, res);
                    } catch (Exception e) {
                        return Optional.of(checkName + ": extraction error - " + e.getMessage());
                    }
                });
    }
}
