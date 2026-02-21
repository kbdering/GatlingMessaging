/*
 * Copyright (c) 2025 Perfluencer. All rights reserved.
 * Contact: kuba@perfluencer.pl
 * 
 * This software is proprietary and confidential. Unauthorized copying,
 * modification, distribution, or use of this software, in whole or in part,
 * is strictly prohibited without the express written permission of Perfluencer.
 */
package pl.perfluencer.kafka.javaapi;

/**
 * Fluent DSL entry point for Kafka actions.
 * 
 * <p>
 * Provides a Gatling-esque pattern for building Kafka actions:
 * 
 * <pre>{@code
 * import static pl.perfluencer.kafka.javaapi.Kafka.*;
 * 
 * kafka("Order Request")
 *     .requestReply()
 *     .requestTopic("orders-request")
 *     .responseTopic("orders-response")
 *     .key(session -> "key")
 *     .value(session -> "{\"orderId\":\"123\"}")
 *     .check(echoCheck())
 *     .timeout(10, SECONDS)
 * }</pre>
 */
public class Kafka {

    private final String requestName;

    private Kafka(String requestName) {
        this.requestName = requestName;
    }

    /**
     * Creates a new Kafka action builder with the specified request name.
     * 
     * @param requestName The name that will appear in Gatling reports
     * @return A Kafka builder for fluent configuration
     */
    public static Kafka kafka(String requestName) {
        return new Kafka(requestName);
    }

    /**
     * Starts building a request-reply action.
     * 
     * <pre>{@code
     * kafka("Order Request")
     *         .requestReply()
     *         .requestTopic("orders-request")
     *         .responseTopic("orders-response")
     *         .key("key")
     *         .value("{\"orderId\":\"123\"}")
     *         .check(echoCheck())
     * }</pre>
     */
    public KafkaRequestReply<byte[], byte[]> requestReply() {
        return new KafkaRequestReply<>(requestName);
    }

    /**
     * Returns the request name for this action.
     */
    public String getRequestName() {
        return requestName;
    }
}
