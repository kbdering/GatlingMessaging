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
