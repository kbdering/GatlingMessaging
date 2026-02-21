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
import java.util.function.Function;

/**
 * Gatling-style fluent typed field extractor for POJO / Protobuf message
 * checks.
 * 
 * <p>
 * Uses Java method references (getter chains) for type-safe field extraction
 * and validation.
 * Works with any deserialized response type: POJOs, Protobuf, Avro, etc.
 * 
 * <h2>Usage:</h2>
 * 
 * <pre>{@code
 * // Single getter extraction
 * field(OrderResponse::getStatus).is("OK")
 * field(OrderResponse::getAmount).gt(100.0)
 * 
 * // Nested getter chain 
 * field(OrderResponse::getDetails).transform(Details::getTrackingId).exists()
 * 
 * // Compare request and response fields
 * fieldEquals(OrderRequest::getOrderId, OrderResponse::getOrderIdEcho)
 * 
 * // Custom comparator
 * fieldCompare(OrderRequest::getAmount, OrderResponse::getChargedAmount,
 *     (req, res) -> Math.abs(req - res) < 0.01)
 * }</pre>
 *
 * @param <ResT> the response message type
 * @author Jakub Dering
 */
public class FieldExtractor<ResT> {

    private final Class<ResT> responseClass;
    private String checkName;

    /**
     * Creates a new field extractor for the given response type.
     *
     * @param responseClass the response class (for type safety)
     */
    public FieldExtractor(Class<ResT> responseClass) {
        this.responseClass = responseClass;
        this.checkName = "field(" + responseClass.getSimpleName() + ")";
    }

    /**
     * Sets a descriptive name for this check.
     */
    public FieldExtractor<ResT> named(String name) {
        this.checkName = name;
        return this;
    }

    // ==================== SINGLE FIELD EXTRACTION ====================

    /**
     * Extracts a field from the response using a getter method reference.
     *
     * <pre>{@code
     * field(OrderResponse.class).get(OrderResponse::getStatus).is("OK")
     * field(OrderResponse.class).get(OrderResponse::getAmount).gt(100)
     * }</pre>
     *
     * @param getter the getter method reference
     * @param <F>    the field type
     * @return CheckStep for fluent validation
     */
    public <F> TypedCheckStep<Void, ResT, F> get(Function<ResT, F> getter) {
        return new TypedCheckStep<>(checkName, responseClass, getter);
    }

    // ==================== REQUEST-RESPONSE COMPARISON ====================

    /**
     * Verifies that a field extracted from request equals the field from response.
     *
     * <pre>{@code
     * field(OrderResponse.class).assertEquals(
     *         OrderRequest::getOrderId, OrderResponse::getOrderId)
     * }</pre>
     *
     * @param requestGetter  extraction from request
     * @param responseGetter extraction from response
     * @param <ReqT>         the request type
     * @param <F>            the field type
     * @return MessageCheckResult
     */
    public <ReqT, F> MessageCheckBuilder.MessageCheckResult<ReqT, ResT> assertEquals(
            Function<ReqT, F> requestGetter,
            Function<ResT, F> responseGetter) {
        return compareFields(requestGetter, responseGetter,
                (reqVal, resVal) -> reqVal == null ? resVal == null : reqVal.equals(resVal));
    }

    /**
     * Compares fields from request and response using a custom comparator.
     *
     * @param requestGetter  extraction from request
     * @param responseGetter extraction from response
     * @param comparator     returns true if values match
     * @param <ReqT>         the request type
     * @param <F>            the field type
     * @return MessageCheckResult
     */
    @SuppressWarnings("unchecked")
    public <ReqT, F> MessageCheckBuilder.MessageCheckResult<ReqT, ResT> compareFields(
            Function<ReqT, F> requestGetter,
            Function<ResT, F> responseGetter,
            java.util.function.BiPredicate<F, F> comparator) {
        return new MessageCheckBuilder.MessageCheckResult<>(
                checkName, (Class<ReqT>) Object.class, responseClass,
                (req, res) -> {
                    try {
                        F reqVal = requestGetter.apply(req);
                        F resVal = responseGetter.apply(res);
                        if (comparator.test(reqVal, resVal)) {
                            return Optional.empty();
                        }
                        return Optional.of(checkName + ": expected '" + reqVal + "' but got '" + resVal + "'");
                    } catch (Exception e) {
                        return Optional.of(checkName + ": field extraction error - " + e.getMessage());
                    }
                });
    }

    // ==================== TYPED CHECK STEP ====================

    /**
     * Intermediate step for typed field extraction that plugs into the CheckStep
     * chain.
     *
     * @param <ReqT> request type (usually Void for response-only checks)
     * @param <ResT> response type
     * @param <F>    extracted field type
     */
    public static class TypedCheckStep<ReqT, ResT, F> {
        private final String checkName;
        private final Class<ResT> responseClass;
        private final Function<ResT, F> getter;

        TypedCheckStep(String checkName, Class<ResT> responseClass, Function<ResT, F> getter) {
            this.checkName = checkName;
            this.responseClass = responseClass;
            this.getter = getter;
        }

        /**
         * Validates that the extracted field equals the expected value.
         */
        @SuppressWarnings("unchecked")
        public MessageCheckBuilder.MessageCheckResult<ReqT, ResT> is(F expected) {
            return new MessageCheckBuilder.MessageCheckResult<>(
                    checkName, (Class<ReqT>) (Class<?>) Void.class, responseClass,
                    (req, res) -> {
                        try {
                            F actual = getter.apply(res);
                            if (expected.equals(actual)) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": expected '" + expected + "' but got '" + actual + "'");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": extraction error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Validates that the extracted field does NOT equal the unexpected value.
         */
        @SuppressWarnings("unchecked")
        public MessageCheckBuilder.MessageCheckResult<ReqT, ResT> not(F unexpected) {
            return new MessageCheckBuilder.MessageCheckResult<>(
                    checkName, (Class<ReqT>) (Class<?>) Void.class, responseClass,
                    (req, res) -> {
                        try {
                            F actual = getter.apply(res);
                            if (!unexpected.equals(actual)) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": should not be '" + unexpected + "'");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": extraction error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Validates that the extracted field is not null.
         */
        @SuppressWarnings("unchecked")
        public MessageCheckBuilder.MessageCheckResult<ReqT, ResT> exists() {
            return new MessageCheckBuilder.MessageCheckResult<>(
                    checkName, (Class<ReqT>) (Class<?>) Void.class, responseClass,
                    (req, res) -> {
                        try {
                            F actual = getter.apply(res);
                            if (actual != null) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": field is null");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": extraction error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Validates that the extracted field satisfies a custom predicate.
         */
        @SuppressWarnings("unchecked")
        public MessageCheckBuilder.MessageCheckResult<ReqT, ResT> satisfies(java.util.function.Predicate<F> predicate) {
            return new MessageCheckBuilder.MessageCheckResult<>(
                    checkName, (Class<ReqT>) (Class<?>) Void.class, responseClass,
                    (req, res) -> {
                        try {
                            F actual = getter.apply(res);
                            if (actual != null && predicate.test(actual)) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": value '" + actual + "' did not satisfy predicate");
                        } catch (Exception e) {
                            return Optional.of(checkName + ": extraction error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Validates that the extracted Comparable field is greater than the threshold.
         */
        @SuppressWarnings("unchecked")
        public MessageCheckBuilder.MessageCheckResult<ReqT, ResT> gt(Comparable<?> threshold) {
            return new MessageCheckBuilder.MessageCheckResult<>(
                    checkName, (Class<ReqT>) (Class<?>) Void.class, responseClass,
                    (req, res) -> {
                        try {
                            F actual = getter.apply(res);
                            if (actual == null) {
                                return Optional.of(checkName + ": field is null, cannot compare");
                            }
                            Comparable<Object> comp = (Comparable<Object>) actual;
                            if (comp.compareTo(threshold) > 0) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": " + actual + " is not > " + threshold);
                        } catch (Exception e) {
                            return Optional.of(checkName + ": comparison error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Validates that the extracted Comparable field is less than the threshold.
         */
        @SuppressWarnings("unchecked")
        public MessageCheckBuilder.MessageCheckResult<ReqT, ResT> lt(Comparable<?> threshold) {
            return new MessageCheckBuilder.MessageCheckResult<>(
                    checkName, (Class<ReqT>) (Class<?>) Void.class, responseClass,
                    (req, res) -> {
                        try {
                            F actual = getter.apply(res);
                            if (actual == null) {
                                return Optional.of(checkName + ": field is null, cannot compare");
                            }
                            Comparable<Object> comp = (Comparable<Object>) actual;
                            if (comp.compareTo(threshold) < 0) {
                                return Optional.empty();
                            }
                            return Optional.of(checkName + ": " + actual + " is not < " + threshold);
                        } catch (Exception e) {
                            return Optional.of(checkName + ": comparison error - " + e.getMessage());
                        }
                    });
        }

        /**
         * Chains to another getter for nested field access.
         *
         * <pre>{@code
         * field(Order.class).get(Order::getDetails).then(Details::getTrackingId).is("TRK-123")
         * }</pre>
         */
        public <G> TypedCheckStep<ReqT, ResT, G> then(Function<F, G> nextGetter) {
            return new TypedCheckStep<>(checkName, responseClass, getter.andThen(nextGetter));
        }
    }
}
