/*
 * Copyright (c) 2025 Perfluencer. All rights reserved.
 * Contact: kuba@perfluencer.pl
 * 
 * This software is proprietary and confidential. Unauthorized copying,
 * modification, distribution, or use of this software, in whole or in part,
 * is strictly prohibited without the express written permission of Perfluencer.
 */
package pl.perfluencer.common.checks;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Gatling-style fluent regex extractor for message checks.
 * 
 * <h2>Usage:</h2>
 * 
 * <pre>{@code
 * // Extract first full match
 * regex("\\d+").find().is("42")
 * 
 * // Extract capture group
 * regex("orderId=(\\w+)").find(1).is("ORD-123")
 * 
 * // Count matches
 * regex("item=\\w+").count().is(3)
 * 
 * // Find all capture groups
 * regex("id=(\\d+)").findAll(1).satisfies(ids -> ids.size() >= 2)
 * 
 * // Transform and validate
 * regex("amount=(\\d+)").find(1).transform(Integer::parseInt).gt(100)
 * }</pre>
 *
 * @author Jakub Dering
 */
public class RegexExtractor {

    private final Pattern pattern;
    private String checkName;

    /**
     * Creates a new regex extractor.
     *
     * @param regex the regular expression pattern
     */
    public RegexExtractor(String regex) {
        this.pattern = Pattern.compile(regex);
        this.checkName = "regex(" + regex + ")";
    }

    /**
     * Sets a descriptive name for this check.
     */
    public RegexExtractor named(String name) {
        this.checkName = name;
        return this;
    }

    // ==================== FIND STRATEGIES ====================

    /**
     * Finds the first full match of the pattern.
     *
     * @return CheckStep containing the first match, or null if not found
     */
    public CheckStep<String> find() {
        return new CheckStep<>(checkName, response -> {
            Matcher matcher = pattern.matcher(response);
            if (matcher.find()) {
                return matcher.group(0);
            }
            return null;
        });
    }

    /**
     * Finds the first match and extracts a capture group.
     *
     * @param group the capture group number (1-based)
     * @return CheckStep containing the group value, or null if not found
     */
    public CheckStep<String> find(int group) {
        return new CheckStep<>(checkName, response -> {
            Matcher matcher = pattern.matcher(response);
            if (matcher.find() && matcher.groupCount() >= group) {
                return matcher.group(group);
            }
            return null;
        });
    }

    /**
     * Finds all matches of the full pattern.
     *
     * @return CheckStep containing a list of all matches
     */
    public CheckStep<List<String>> findAll() {
        return new CheckStep<>(checkName, response -> {
            List<String> results = new ArrayList<>();
            Matcher matcher = pattern.matcher(response);
            while (matcher.find()) {
                results.add(matcher.group(0));
            }
            return results.isEmpty() ? null : results;
        });
    }

    /**
     * Finds all matches and extracts a capture group from each.
     *
     * @param group the capture group number (1-based)
     * @return CheckStep containing a list of group values
     */
    public CheckStep<List<String>> findAll(int group) {
        return new CheckStep<>(checkName, response -> {
            List<String> results = new ArrayList<>();
            Matcher matcher = pattern.matcher(response);
            while (matcher.find()) {
                if (matcher.groupCount() >= group) {
                    results.add(matcher.group(group));
                }
            }
            return results.isEmpty() ? null : results;
        });
    }

    /**
     * Counts the number of matches of the pattern.
     *
     * @return CheckStep containing the match count
     */
    public CheckStep<Integer> count() {
        return new CheckStep<>(checkName, response -> {
            int count = 0;
            Matcher matcher = pattern.matcher(response);
            while (matcher.find()) {
                count++;
            }
            return count;
        });
    }
}
