/*
 * Copyright (c) 2025 Perfluencer. All rights reserved.
 * Contact: kuba@perfluencer.pl
 * 
 * This software is proprietary and confidential. Unauthorized copying,
 * modification, distribution, or use of this software, in whole or in part,
 * is strictly prohibited without the express written permission of Perfluencer.
 */
package pl.perfluencer.common.checks;

/**
 * Gatling-style fluent substring extractor for message checks.
 * 
 * <h2>Usage:</h2>
 * 
 * <pre>{@code
 * // Check substring exists
 * substring("SUCCESS").find().exists()
 * 
 * // Count occurrences
 * substring("ERROR").count().is(0)
 * 
 * // Validate the substring is present
 * substring("transactionId").find().is("transactionId")
 * }</pre>
 *
 * @author Jakub Dering
 */
public class SubstringExtractor {

    private final String text;
    private String checkName;

    /**
     * Creates a new substring extractor.
     *
     * @param text the substring to search for
     */
    public SubstringExtractor(String text) {
        this.text = text;
        this.checkName = "substring(" + text + ")";
    }

    /**
     * Sets a descriptive name for this check.
     */
    public SubstringExtractor named(String name) {
        this.checkName = name;
        return this;
    }

    // ==================== FIND STRATEGIES ====================

    /**
     * Finds the substring in the response.
     *
     * @return CheckStep containing the substring if found, or null if not present
     */
    public CheckStep<String> find() {
        return new CheckStep<>(checkName, response -> {
            if (response != null && response.contains(text)) {
                return text;
            }
            return null;
        });
    }

    /**
     * Counts the number of non-overlapping occurrences of the substring.
     *
     * @return CheckStep containing the occurrence count
     */
    public CheckStep<Integer> count() {
        return new CheckStep<>(checkName, response -> {
            if (response == null) {
                return 0;
            }
            int count = 0;
            int idx = 0;
            while ((idx = response.indexOf(text, idx)) != -1) {
                count++;
                idx += text.length();
            }
            return count;
        });
    }
}
