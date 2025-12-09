package pl.perfluencer.kafka.extractors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Strategy interface for extracting correlation IDs from Kafka consumer
 * records.
 * 
 * <p>
 * In request-reply patterns, each response must be matched back to its original
 * request.
 * The correlation ID is the key that makes this possible. However, different
 * systems
 * store this ID in different places: some in headers, some in the message body.
 * 
 * <h2>Built-in Implementations:</h2>
 * <dl>
 * <dt>Default (Header-Based)</dt>
 * <dd>Looks for a header named "correlationId" - this is the most common
 * pattern</dd>
 * 
 * <dt>{@link JsonPathExtractor}</dt>
 * <dd>Extracts correlation ID from JSON message bodies using JSONPath
 * expressions
 * (e.g., "$.metadata.id" or "$.headers.correlationId")</dd>
 * 
 * <dt>{@link FunctionExtractor}</dt>
 * <dd>Accepts a custom lambda/function for maximum flexibility</dd>
 * </dl>
 * 
 * <h2>Usage Example:</h2>
 * 
 * <pre>{@code
 * // Using JSONPath extractor for body-based correlation
 * KafkaProtocolBuilder protocol = kafka()
 *         .correlationExtractor(new JsonPathExtractor("$.request.id"))
 *         .bootstrapServers("localhost:9092");
 * 
 * // Using function extractor for custom logic
 * KafkaProtocolBuilder protocol = kafka()
 *         .correlationExtractor(new FunctionExtractor(record -> {
 *             // Custom extraction logic
 *             MyCustomMessage msg = deserialize(record.value());
 *             return msg.getTransactionId();
 *         }))
 *         .bootstrapServers("localhost:9092");
 * }</pre>
 * 
 * <h2>Implementation Notes:</h2>
 * <ul>
 * <li>Must be thread-safe (will be called from multiple consumer threads)</li>
 * <li>Should be fast (called for every message received)</li>
 * <li>Should return {@code null} if correlation ID cannot be extracted</li>
 * </ul>
 * 
 * @author Jakub Dering
 * @see JsonPathExtractor
 * @see FunctionExtractor
 */
public interface CorrelationExtractor {
    /**
     * Extracts the correlation ID from a Kafka consumer record.
     * 
     * <p>
     * This method is called for every response message received from Kafka.
     * The returned correlation ID is used to look up the original request in the
     * {@link pl.perfluencer.kafka.cache.RequestStore}.
     * 
     * @param record the Kafka consumer record containing the response message
     * @return the correlation ID as a string, or {@code null} if it cannot be
     *         extracted
     */
    String extract(ConsumerRecord<String, Object> record);
}
