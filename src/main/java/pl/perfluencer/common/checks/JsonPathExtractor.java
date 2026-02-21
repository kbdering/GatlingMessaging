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

import com.jayway.jsonpath.JsonPath;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Gatling-style fluent JSONPath extractor for message checks.
 * 
 * <p>
 * Uses Jayway JSONPath for extraction, plugging into the standard
 * {@link CheckStep} chain for validation.
 * 
 * <h2>Usage:</h2>
 * 
 * <pre>{@code
 * // Extract and validate a value
 * jsonPath("$.status").find().is("OK")
 * jsonPath("$.orderId").find().exists()
 * 
 * // Numeric comparison with transform
 * jsonPath("$.amount").ofInt().gt(100)
 * jsonPath("$.price").ofDouble().lt(99.99)
 * 
 * // Count array elements
 * jsonPath("$.items[*]").count().gt(0)
 * 
 * // Find all values
 * jsonPath("$.items[*].name").findAll().satisfies(names -> names.contains("Widget"))
 * }</pre>
 *
 * @author Jakub Dering
 */
public class JsonPathExtractor {

    private final String expression;
    private String checkName;

    /**
     * Creates a new JSONPath extractor.
     *
     * @param expression the JSONPath expression (e.g. "$.status",
     *                   "$.items[*].name")
     */
    public JsonPathExtractor(String expression) {
        this.expression = expression;
        this.checkName = "jsonPath(" + expression + ")";
    }

    /**
     * Sets a descriptive name for this check.
     */
    public JsonPathExtractor named(String name) {
        this.checkName = name;
        return this;
    }

    // ==================== FIND STRATEGIES ====================

    /**
     * Finds the value at the JSONPath and returns it as a String.
     *
     * @return CheckStep containing the value as String, or null if not found
     */
    public CheckStep<String> find() {
        return new CheckStep<>(checkName, response -> {
            try {
                Object value = JsonPath.read(response, expression);
                if (value == null) {
                    return null;
                }
                if (value instanceof Collection) {
                    Collection<?> arr = (Collection<?>) value;
                    return arr.isEmpty() ? null : String.valueOf(arr.iterator().next());
                }
                return String.valueOf(value);
            } catch (Exception e) {
                return null;
            }
        });
    }

    /**
     * Finds the value at the JSONPath and returns it as the raw Object.
     * Useful for comparing against typed expected values directly.
     *
     * @return CheckStep containing the raw parsed value
     */
    public CheckStep<Object> findRaw() {
        return new CheckStep<>(checkName, response -> {
            try {
                return JsonPath.read(response, expression);
            } catch (Exception e) {
                return null;
            }
        });
    }

    /**
     * Finds the value at the JSONPath and returns it as an Integer.
     *
     * @return CheckStep containing the integer value
     */
    public CheckStep<Integer> ofInt() {
        return new CheckStep<>(checkName, response -> {
            try {
                Object value = JsonPath.read(response, expression);
                if (value == null)
                    return null;
                if (value instanceof Number)
                    return ((Number) value).intValue();
                return Integer.parseInt(String.valueOf(value));
            } catch (Exception e) {
                return null;
            }
        });
    }

    /**
     * Finds the value at the JSONPath and returns it as a Double.
     *
     * @return CheckStep containing the double value
     */
    public CheckStep<Double> ofDouble() {
        return new CheckStep<>(checkName, response -> {
            try {
                Object value = JsonPath.read(response, expression);
                if (value == null)
                    return null;
                if (value instanceof Number)
                    return ((Number) value).doubleValue();
                return Double.parseDouble(String.valueOf(value));
            } catch (Exception e) {
                return null;
            }
        });
    }

    /**
     * Finds the value at the JSONPath and returns it as a Long.
     *
     * @return CheckStep containing the long value
     */
    public CheckStep<Long> ofLong() {
        return new CheckStep<>(checkName, response -> {
            try {
                Object value = JsonPath.read(response, expression);
                if (value == null)
                    return null;
                if (value instanceof Number)
                    return ((Number) value).longValue();
                return Long.parseLong(String.valueOf(value));
            } catch (Exception e) {
                return null;
            }
        });
    }

    /**
     * Finds all values matching the JSONPath expression.
     *
     * @return CheckStep containing a list of String values
     */
    public CheckStep<List<String>> findAll() {
        return new CheckStep<>(checkName, response -> {
            try {
                Object value = JsonPath.read(response, expression);
                if (value == null)
                    return null;
                if (value instanceof Collection) {
                    Collection<?> arr = (Collection<?>) value;
                    if (arr.isEmpty())
                        return null;
                    List<String> results = new ArrayList<>();
                    for (Object item : arr) {
                        results.add(String.valueOf(item));
                    }
                    return results;
                }
                // Single value → wrap in list
                List<String> single = new ArrayList<>();
                single.add(String.valueOf(value));
                return single;
            } catch (Exception e) {
                return null;
            }
        });
    }

    /**
     * Counts the number of values matching the JSONPath expression.
     * For arrays, returns array length. For single values, returns 1. For missing,
     * returns 0.
     *
     * @return CheckStep containing the count
     */
    public CheckStep<Integer> count() {
        return new CheckStep<>(checkName, response -> {
            try {
                Object value = JsonPath.read(response, expression);
                if (value == null)
                    return 0;
                if (value instanceof Collection) {
                    return ((Collection<?>) value).size();
                }
                return 1;
            } catch (Exception e) {
                return 0;
            }
        });
    }
}
