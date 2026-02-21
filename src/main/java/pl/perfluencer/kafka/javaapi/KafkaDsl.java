package pl.perfluencer.kafka.javaapi;

import io.gatling.core.protocol.Protocol;
import io.gatling.javaapi.core.Session;
import io.gatling.javaapi.core.internal.Expressions;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.actions.KafkaActionBuilder;
import pl.perfluencer.kafka.actions.KafkaConsumeActionBuilder;
import pl.perfluencer.kafka.actions.KafkaRequestReplyActionBuilder;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;

import pl.perfluencer.kafka.extractors.JsonPathExtractor;
import pl.perfluencer.common.util.SerializationType;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Main DSL entry point for the Gatling Kafka extension.
 * 
 * <p>
 * This class provides a fluent API for creating Kafka-based load test scenarios
 * in Gatling.
 * It supports both fire-and-forget message sending and request-reply patterns
 * with correlation.
 * 
 * <h2>Usage Examples:</h2>
 * 
 * <h3>Simple Message Send:</h3>
 * 
 * <pre>{@code
 * exec(kafka("my-topic", "key", "value"))
 * }</pre>
 * 
 * <h3>Request-Reply Pattern:</h3>
 * 
 * <pre>{@code
 * exec(kafkaRequestReply("request-topic", "response-topic",
 *                 session -> "myKey",
 *                 session -> "myValue",
 *                 SerializationType.STRING,
 *                 checks,
 *                 10, TimeUnit.SECONDS))
 * }</pre>
 * 
 * @author Jakub Dering
 * @see KafkaProtocolBuilder
 * @see KafkaActionBuilder
 * @see KafkaRequestReplyActionBuilder
 */
public class KafkaDsl {

        private final String requestName;

        public KafkaDsl(String requestName) {
                this.requestName = requestName;
        }

        // ==================== FLUENT API ENTRY POINTS ====================

        /**
         * Entry point for the fluent Kafka DSL.
         * 
         * <pre>{@code
         * kafka("Order Request")
         *                 .requestReply()
         *                 .requestTopic("req")
         *                 .responseTopic("res")
         *                 .send("payload")
         * }</pre>
         */
        public static KafkaDsl kafka(String requestName) {
                return new KafkaDsl(requestName);
        }

        /**
         * Start a Request-Reply definition.
         */
        public KafkaRequestReply<byte[], byte[]> requestReply() {
                return new KafkaRequestReply<>(requestName);
        }

        /**
         * Start a Fire-and-Forget send definition.
         *
         * <pre>{@code
         * kafka("Send Avro")
         *                 .send()
         *                 .topic("users")
         *                 .key(session -> UUID.randomUUID().toString())
         *                 .value(session -> createGenericRecord())
         *                 .asAvro()
         * }</pre>
         */
        public KafkaFireAndForget send() {
                return new KafkaFireAndForget(requestName);
        }

        // ==================== LEGACY / STATIC API ====================

        /**
         * Creates a new Kafka protocol builder.
         * 
         * <p>
         * This is the starting point for configuring Kafka protocol settings such as
         * bootstrap servers, number of producers/consumers, and request stores.
         * 
         * @return a new {@link KafkaProtocolBuilder} instance
         */
        public static KafkaProtocolBuilder kafka() {
                return new KafkaProtocolBuilder();
        }

        /**
         * Creates a correlation extractor that uses JSONPath to extract correlation IDs
         * from message bodies.
         * 
         * <p>
         * This is useful when the correlation ID is not in the message headers but
         * embedded
         * within a JSON message body.
         * 
         * <h3>Example:</h3>
         * 
         * <pre>{@code
         * KafkaProtocolBuilder protocol = kafka()
         *                 .correlationExtractor(jsonPath("$.metadata.correlationId"))
         *                 .bootstrapServers("localhost:9092");
         * }</pre>
         * 
         * @param path the JSONPath expression to extract the correlation ID (e.g.,
         *             "$.id" or "$.meta.correlationId")
         * @return a {@link CorrelationExtractor} that uses JSONPath
         * @see JsonPathExtractor
         */
        public static CorrelationExtractor jsonPath(String path) {
                return new JsonPathExtractor(path);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages to a Kafka topic. Uses
         * Expressions
         * for key and value to allow dynamic values from the Gatling session.
         *
         * @param topic The target Kafka topic.
         * @param key   An expression string that resolves to the message key.
         * @param value An expression string that resolves to the message value.
         * @return A KafkaActionBuilder for sending messages.
         */
        public static KafkaActionBuilder kafka(String topic, String key, String value) {
                return new KafkaActionBuilder(null, topic,
                                toJavaFunction(key),
                                toJavaFunction(value), null, null);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages to a Kafka topic with a
         * custom request name.
         *
         * @param requestName The name of the request.
         * @param topic       The target Kafka topic.
         * @param key         An expression string that resolves to the message key.
         * @param value       An expression string that resolves to the message value.
         * @return A KafkaActionBuilder for sending messages.
         */
        public static KafkaActionBuilder kafka(String requestName, String topic, String key, String value) {
                return new KafkaActionBuilder(requestName, topic,
                                toJavaFunction(key),
                                toJavaFunction(value), null, null);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages to a Kafka topic. Uses
         * Expressions
         * for key and value to allow dynamic values from the Gatling session.
         *
         * @param topic      The target Kafka topic.
         * @param key        An expression string that resolves to the message key.
         * @param value      An expression string that resolves to the message value.
         * @param waitForAck boolean that defines if it is necessary to wait for
         *                   acknowledgement.
         * @param timeout    timeout value.
         * @param timeUnit   timeout unit.
         * @return A KafkaActionBuilder for sending messages.
         */

        public static KafkaActionBuilder kafka(String topic, String key, String value, boolean waitForAck, long timeout,
                        TimeUnit timeUnit) {
                return new KafkaActionBuilder(null, topic,
                                toJavaFunction(key),
                                toJavaFunction(value), null, waitForAck,
                                timeout, timeUnit);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages to a Kafka topic with a
         * custom request name.
         *
         * @param requestName The name of the request.
         * @param topic       The target Kafka topic.
         * @param key         An expression string that resolves to the message key.
         * @param value       An expression string that resolves to the message value.
         * @param waitForAck  boolean that defines if it is necessary to wait for
         *                    acknowledgement.
         * @param timeout     timeout value.
         * @param timeUnit    timeout unit.
         * @return A KafkaActionBuilder for sending messages.
         */
        public static KafkaActionBuilder kafka(String requestName, String topic, String key, String value,
                        boolean waitForAck, long timeout, TimeUnit timeUnit) {
                return new KafkaActionBuilder(requestName, topic,
                                toJavaFunction(key),
                                toJavaFunction(value), null, waitForAck,
                                timeout, timeUnit);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages, using lambdas (Session ->
         * String)
         * for dynamic keys and values.
         *
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that takes a Gatling Session and returns the
         *                      message key.
         * @param valueFunction A function that takes a Gatling Session and returns the
         *                      message value.
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafka(String topic, java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, String> valueFunction) {
                return new KafkaActionBuilder(null, topic, keyFunction, valueFunction, null, null);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages, using lambdas (Session ->
         * String)
         * for dynamic keys and values with a custom request name.
         *
         * @param requestName   The name of the request.
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that takes a Gatling Session and returns the
         *                      message key.
         * @param valueFunction A function that takes a Gatling Session and returns the
         *                      message value.
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafka(String requestName, String topic,
                        java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, String> valueFunction) {
                return new KafkaActionBuilder(requestName, topic, keyFunction, valueFunction, null, null);
        }

        // ====================================================================
        // Fire-and-forget DSL with Object value (byte[], Avro, Protobuf, etc.)
        // ====================================================================

        /**
         * Creates a KafkaActionBuilder for sending messages with an Object value.
         * <p>
         * Use this for non-String payloads such as byte[], Avro GenericRecord,
         * or Protobuf Message. The Kafka producer serializer configured in
         * the protocol handles the actual serialization.
         * </p>
         *
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that takes a Gatling Session and returns the
         *                      message key (String).
         * @param valueFunction A function that takes a Gatling Session and returns the
         *                      message value (any Object).
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafkaObject(String topic,
                        java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, Object> valueFunction) {
                return KafkaActionBuilder.withObjectValue(null, topic, keyFunction, valueFunction);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages with an Object value
         * and a custom request name.
         *
         * @param requestName   The name of the request.
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that takes a Gatling Session and returns the
         *                      message key (String).
         * @param valueFunction A function that takes a Gatling Session and returns the
         *                      message value (any Object).
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafkaObject(String requestName, String topic,
                        java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, Object> valueFunction) {
                return KafkaActionBuilder.withObjectValue(requestName, topic, keyFunction, valueFunction);
        }

        /**
         * Creates a KafkaActionBuilder for sending byte[] values.
         *
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that returns the message key.
         * @param valueFunction A function that returns the message value as byte[].
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafkaBytes(String topic,
                        java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, byte[]> valueFunction) {
                return KafkaActionBuilder.withObjectValue(null, topic, keyFunction,
                                session -> valueFunction.apply(session));
        }

        /**
         * Creates a KafkaActionBuilder for sending byte[] values with a custom
         * request name.
         *
         * @param requestName   The name of the request.
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that returns the message key.
         * @param valueFunction A function that returns the message value as byte[].
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafkaBytes(String requestName, String topic,
                        java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, byte[]> valueFunction) {
                return KafkaActionBuilder.withObjectValue(requestName, topic, keyFunction,
                                session -> valueFunction.apply(session));
        }

        /**
         * Creates a KafkaActionBuilder for sending messages, using lambdas (Session ->
         * String)
         * for dynamic keys and values.
         *
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that takes a Gatling Session and returns the
         *                      message key.
         * @param valueFunction A function that takes a Gatling Session and returns the
         *                      message value.
         * @param waitForAck    boolean that defines if it is necessary to wait for
         *                      acknowledgement.
         * @param timeout       timeout value.
         * @param timeUnit      timeout unit.
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafka(String topic, java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, String> valueFunction,
                        boolean waitForAck, long timeout, TimeUnit timeUnit) {
                return new KafkaActionBuilder(null, topic, keyFunction, valueFunction, null, waitForAck, timeout,
                                timeUnit);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages, using lambdas (Session ->
         * String)
         * for dynamic keys and values with a custom request name.
         *
         * @param requestName   The name of the request.
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that takes a Gatling Session and returns the
         *                      message key.
         * @param valueFunction A function that takes a Gatling Session and returns the
         *                      message value.
         * @param waitForAck    boolean that defines if it is necessary to wait for
         *                      acknowledgement.
         * @param timeout       timeout value.
         * @param timeUnit      timeout unit.
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafka(String requestName, String topic,
                        java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, String> valueFunction,
                        boolean waitForAck, long timeout, TimeUnit timeUnit) {
                return new KafkaActionBuilder(requestName, topic, keyFunction, valueFunction, null, waitForAck, timeout,
                                timeUnit);
        }

        // Existing method, adapted to use byte[] for String value, assuming STRING
        // serialization and no checks by default
        public static KafkaRequestReplyActionBuilder<byte[], byte[]> kafkaRequestReply(String requestTopic,
                        String responseTopic,
                        Function<Session, String> keyFunction,
                        String valueExpression, // Gatling EL for string value
                        Protocol protocol,
                        long timeout, TimeUnit timeUnit) {
                Function<Session, String> stringValueFunction = toJavaFunction(valueExpression);
                Function<Session, Object> byteValueFunction = session -> stringValueFunction.apply(session)
                                .getBytes(StandardCharsets.UTF_8);
                return new KafkaRequestReplyActionBuilder<>("kafka-request-reply-action", requestTopic,
                                responseTopic, keyFunction, byteValueFunction, null,
                                byte[].class, SerializationType.STRING, byte[].class, SerializationType.STRING,
                                protocol, Collections.emptyList(), true, timeout, timeUnit);
        }

        // Recommended: Overloaded method without the Protocol parameter
        public static KafkaRequestReplyActionBuilder<byte[], byte[]> kafkaRequestReply(String requestTopic,
                        String responseTopic,
                        Function<Session, String> keyFunction,
                        Function<Session, Object> valueFunction,
                        SerializationType requestSerializationType,
                        List<MessageCheck<?, ?>> messageChecks,
                        long timeout, TimeUnit timeUnit) {
                // Pass null for the protocol; the ActionBuilder will resolve it from the
                // context.
                return new KafkaRequestReplyActionBuilder<>("kafka-request-reply-action", requestTopic,
                                responseTopic, keyFunction, valueFunction, null,
                                byte[].class, requestSerializationType, byte[].class, requestSerializationType,
                                null, messageChecks, true, timeout, timeUnit);
        }

        // New comprehensive method for byte[] value, explicit SerializationType, and
        // MessageChecks
        public static KafkaRequestReplyActionBuilder<byte[], byte[]> kafkaRequestReply(String requestTopic,
                        String responseTopic,
                        Function<Session, String> keyFunction,
                        Function<Session, Object> valueFunction,
                        SerializationType requestSerializationType,
                        List<MessageCheck<?, ?>> messageChecks, // Changed to generic wildcard
                        Protocol protocol,
                        long timeout, TimeUnit timeUnit) {
                return new KafkaRequestReplyActionBuilder<>("kafka-request-reply-action", requestTopic,
                                responseTopic, keyFunction, valueFunction, null,
                                byte[].class, requestSerializationType, byte[].class, requestSerializationType,
                                protocol, messageChecks, true, timeout, timeUnit);
        }

        // New methods with requestName support

        private static Function<Session, String> toJavaFunction(String expression) {
                scala.Function1<io.gatling.core.session.Session, io.gatling.commons.validation.Validation<String>> scalaExpr = Expressions
                                .toStringExpression(expression);

                return javaSession -> {
                        io.gatling.core.session.Session scalaSession = javaSession.asScala();
                        io.gatling.commons.validation.Validation<String> result = scalaExpr.apply(scalaSession);
                        if (result instanceof io.gatling.commons.validation.Success) {
                                return ((io.gatling.commons.validation.Success<String>) result).value();
                        } else {
                                Object failure = result;
                                if (failure instanceof io.gatling.commons.validation.Failure) {
                                        throw new RuntimeException(
                                                        ((io.gatling.commons.validation.Failure) failure).message());
                                }
                                throw new RuntimeException("Expression resolution failed: " + result);
                        }
                };
        }

        public static KafkaRequestReplyActionBuilder<byte[], byte[]> kafkaRequestReply(String requestName,
                        String requestTopic,
                        String responseTopic,
                        Function<Session, String> keyFunction,
                        String valueExpression, // Gatling EL for string value
                        Protocol protocol,
                        long timeout, TimeUnit timeUnit) {
                Function<Session, String> stringValueFunction = toJavaFunction(valueExpression);
                Function<Session, Object> byteValueFunction = session -> stringValueFunction.apply(session)
                                .getBytes(StandardCharsets.UTF_8);
                return new KafkaRequestReplyActionBuilder<>(requestName, requestTopic, responseTopic, keyFunction,
                                byteValueFunction, null,
                                byte[].class, SerializationType.STRING, byte[].class, SerializationType.STRING,
                                protocol, Collections.emptyList(), true, timeout, timeUnit);
        }

        public static KafkaRequestReplyActionBuilder<byte[], byte[]> kafkaRequestReply(String requestName,
                        String requestTopic,
                        String responseTopic,
                        Function<Session, String> keyFunction,
                        Function<Session, Object> valueFunction,
                        SerializationType requestSerializationType,
                        List<MessageCheck<?, ?>> messageChecks,
                        long timeout, TimeUnit timeUnit) {
                // Pass null for the protocol; the ActionBuilder will resolve it from the
                // context.
                return new KafkaRequestReplyActionBuilder<>(requestName, requestTopic, responseTopic, keyFunction,
                                valueFunction, null,
                                byte[].class, requestSerializationType, byte[].class, requestSerializationType,
                                null, messageChecks, true, timeout, timeUnit);
        }

        public static KafkaRequestReplyActionBuilder<byte[], byte[]> kafkaRequestReply(String requestName,
                        String requestTopic,
                        String responseTopic,
                        Function<Session, String> keyFunction,
                        Function<Session, Object> valueFunction,
                        SerializationType requestSerializationType,
                        List<MessageCheck<?, ?>> messageChecks, // Changed to generic wildcard
                        Protocol protocol,
                        long timeout, TimeUnit timeUnit) {
                return new KafkaRequestReplyActionBuilder<>(requestName, requestTopic, responseTopic, keyFunction,
                                valueFunction, null,
                                byte[].class, requestSerializationType, byte[].class, requestSerializationType,
                                protocol, messageChecks, true, timeout, timeUnit);
        }

        // Overloads with waitForAck

        public static KafkaRequestReplyActionBuilder<byte[], byte[]> kafkaRequestReply(String requestName,
                        String requestTopic,
                        String responseTopic,
                        Function<Session, String> keyFunction,
                        Function<Session, Object> valueFunction,
                        SerializationType requestSerializationType,
                        List<MessageCheck<?, ?>> messageChecks,
                        boolean waitForAck,
                        long timeout, TimeUnit timeUnit) {
                return new KafkaRequestReplyActionBuilder<>(requestName, requestTopic, responseTopic, keyFunction,
                                valueFunction, null,
                                byte[].class, requestSerializationType, byte[].class, requestSerializationType,
                                null, messageChecks, waitForAck, timeout, timeUnit);
        }

        /**
         * Creates a KafkaActionBuilder for sending messages, using lambdas (Session ->
         * String)
         * for dynamic keys and values with a custom request name and headers.
         *
         * @param requestName   The name of the request.
         * @param topic         The target Kafka topic.
         * @param keyFunction   A function that takes a Gatling Session and returns the
         *                      message key.
         * @param valueFunction A function that takes a Gatling Session and returns the
         *                      message value.
         * @param headers       A map of header names to functions that resolve header
         *                      values from the session.
         * @param waitForAck    boolean that defines if it is necessary to wait for
         *                      acknowledgement.
         * @param timeout       timeout value.
         * @param timeUnit      timeout unit.
         * @return A KafkaActionBuilder.
         */
        public static KafkaActionBuilder kafka(String requestName, String topic,
                        java.util.function.Function<Session, String> keyFunction,
                        java.util.function.Function<Session, String> valueFunction,
                        java.util.Map<String, java.util.function.Function<Session, String>> headers,
                        boolean waitForAck, long timeout, TimeUnit timeUnit) {
                return new KafkaActionBuilder(requestName, topic, keyFunction, valueFunction, headers, waitForAck,
                                timeout,
                                timeUnit);
        }

        public static KafkaRequestReplyActionBuilder<byte[], byte[]> kafkaRequestReply(String requestName,
                        String requestTopic,
                        String responseTopic,
                        Function<Session, String> keyFunction,
                        Function<Session, Object> valueFunction,
                        java.util.Map<String, java.util.function.Function<Session, String>> headers,
                        SerializationType requestSerializationType,
                        List<MessageCheck<?, ?>> messageChecks,
                        boolean waitForAck,
                        long timeout, TimeUnit timeUnit) {
                return new KafkaRequestReplyActionBuilder<>(requestName, requestTopic, responseTopic, keyFunction,
                                valueFunction, headers,
                                byte[].class, requestSerializationType, byte[].class, requestSerializationType,
                                null, messageChecks, waitForAck, timeout, timeUnit);
        }

        // ==================== CONSUME-ONLY API ====================

        /**
         * Creates a consume-only action builder.
         *
         * <p>
         * Starts consumer threads in the background that consume from the given topic
         * and apply registered checks to every record. No messages are sent.
         * </p>
         *
         * <pre>{@code
         * exec(KafkaDsl.consume("Consume Orders", "orders-topic")
         *                 .check(MessageCheck.responseNotEmpty())
         *                 .check(MessageCheck.responseContains("orderId")))
         * }</pre>
         *
         * @param requestName the name for stats reporting and check registry lookup
         * @param topic       the topic to consume from
         * @return a {@link KafkaConsumeActionBuilder}
         */
        public static KafkaConsumeActionBuilder<byte[]> consume(String requestName, String topic) {
                return new KafkaConsumeActionBuilder<>(requestName, topic);
        }

        /**
         * Creates a consume-only action builder using the topic as the request name.
         *
         * @param topic the topic to consume from (also used as the request name)
         * @return a {@link KafkaConsumeActionBuilder}
         */
        public static KafkaConsumeActionBuilder<byte[]> consume(String topic) {
                return new KafkaConsumeActionBuilder<>(topic, topic);
        }

}