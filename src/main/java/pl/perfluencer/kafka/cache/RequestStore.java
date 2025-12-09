package pl.perfluencer.kafka.cache;

import java.util.List;
import java.util.Map;
import pl.perfluencer.kafka.util.SerializationType;

/**
 * Persistence layer for tracking in-flight request-reply transactions in Kafka
 * load tests.
 * 
 * <p>
 * The {@code RequestStore} is responsible for storing request metadata and
 * payloads
 * so that when a response arrives (potentially on a different thread or even a
 * different
 * Gatling instance), the framework can correlate it back to the original
 * request and measure
 * the true end-to-end latency.
 * 
 * <h2>Why This Matters:</h2>
 * <p>
 * In Kafka request-reply patterns, the producer and consumer operate
 * independently:
 * <ul>
 * <li>Request sent → stored in RequestStore with correlation ID</li>
 * <li>Response received → correlation ID extracted</li>
 * <li>RequestStore queried → original request retrieved</li>
 * <li>Validation performed → transaction completed</li>
 * </ul>
 * 
 * <h2>Available Implementations:</h2>
 * <dl>
 * <dt>{@link InMemoryRequestStore}</dt>
 * <dd>Fast, in-process storage using ConcurrentHashMap. Data lost on restart.
 * Best for: Development, short-lived tests, single Gatling instance.</dd>
 * 
 * <dt>{@link RedisRequestStore}</dt>
 * <dd>High-performance distributed storage using Redis. Requires Redis server.
 * Best for: High throughput scenarios, multiple Gatling instances, resilience
 * testing.</dd>
 * 
 * <dt>{@link PostgresRequestStore}</dt>
 * <dd>Durable distributed storage using PostgreSQL. Requires database setup.
 * Best for: Long-running tests, audit trails, maximum durability
 * guarantees.</dd>
 * </dl>
 * 
 * <h2>Data Model:</h2>
 * <p>
 * Each stored request contains:
 * <ul>
 * <li>{@code correlationId} - Unique identifier for matching
 * request/response</li>
 * <li>{@code key} - The Kafka message key</li>
 * <li>{@code value} - The serialized message payload (byte[])</li>
 * <li>{@code serializationType} - How to deserialize the payload</li>
 * <li>{@code transactionName} - Gatling transaction name for reporting</li>
 * <li>{@code scenarioName} - Gatling scenario name for reporting</li>
 * <li>{@code startTime} - When the request was sent (for latency
 * calculation)</li>
 * <li>{@code timeoutTime} - When this request should be considered timed
 * out</li>
 * </ul>
 * 
 * <h2>Timeout Handling:</h2>
 * <p>
 * Implementations should support automatic timeout detection via
 * {@link #startTimeoutMonitoring(TimeoutHandler)}.
 * This prevents memory leaks from requests that never receive a response (e.g.,
 * due to application crashes).
 * 
 * <h2>Thread Safety:</h2>
 * <p>
 * All implementations MUST be thread-safe, as multiple Gatling actors will be
 * storing
 * and retrieving requests concurrently.
 * 
 * @author Jakub Dering
 * @see InMemoryRequestStore
 * @see RedisRequestStore
 * @see PostgresRequestStore
 * @see TimeoutHandler
 */
public interface RequestStore extends AutoCloseable {
    /** Map key for the message key. */
    String KEY = "key";
    /** Map key for the serialized value bytes. */
    String VALUE_BYTES = "valueBytes";
    /** Map key for the serialization type. */
    String SERIALIZATION_TYPE = "serializationType";
    /** Map key for the transaction name. */
    String TRANSACTION_NAME = "transactionName";
    /** Map key for the scenario name. */
    String SCENARIO_NAME = "scenarioName";
    /** Map key for the start time (timestamp in milliseconds). */
    String START_TIME = "startTime";

    /**
     * Stores a request for later correlation with its response.
     * 
     * <p>
     * This method is called immediately after sending a message to Kafka and before
     * waiting for the response. The stored data allows the framework to match the
     * response
     * when it arrives and calculate the true end-to-end latency.
     * 
     * @param correlationId     unique identifier for matching this request with its
     *                          response
     * @param key               the Kafka message key
     * @param value             the message payload (will be serialized to byte[])
     * @param serializationType how the value should be serialized/deserialized
     * @param transactionName   the Gatling transaction name for reporting
     * @param scenarioName      the Gatling scenario name for reporting
     * @param startTime         timestamp when the request was sent (milliseconds
     *                          since epoch)
     * @param timeoutMillis     how long to wait before considering this request
     *                          timed out
     */
    void storeRequest(String correlationId, String key, Object value, SerializationType serializationType,
            String transactionName, String scenarioName, long startTime, long timeoutMillis);

    /**
     * Retrieves a single stored request by its correlation ID.
     * 
     * <p>
     * This method is called when a response is received from Kafka to look up the
     * original request data for validation and latency calculation.
     * 
     * @param correlationId the unique identifier of the request
     * @return a map containing the request data (see class-level constants for
     *         keys),
     *         or {@code null} if no request exists with this correlation ID
     */
    Map<String, Object> getRequest(String correlationId); // Value will be byte[], type will be SerializationType

    /**
     * Retrieves multiple stored requests in a single operation (batch retrieval).
     * 
     * <p>
     * This method is an optimization for scenarios where multiple responses arrive
     * simultaneously and need to be matched with their requests efficiently.
     * 
     * @param correlationIds list of correlation IDs to retrieve
     * @return a map where keys are correlation IDs and values are request data maps
     */
    Map<String, Map<String, Object>> getRequests(List<String> correlationIds);

    /**
     * Processes batched consumer records efficiently using a callback function.
     * 
     * <p>
     * This method allows implementations to optimize batch processing, for example
     * by retrieving all required requests in a single database query and then
     * invoking
     * the processor for each matched pair.
     * 
     * @param records map of correlation IDs to consumer record objects
     * @param process callback function to invoke for each matched request-response
     *                pair
     */
    void processBatchedRecords(Map<String, Object> records, BatchProcessor process);

    /**
     * Deletes a request from the store after it has been successfully processed.
     * 
     * <p>
     * This method should be called after the response has been validated and the
     * transaction has been completed to prevent memory leaks.
     * 
     * @param correlationId the correlation ID of the request to delete
     */
    void deleteRequest(String correlationId);

    /**
     * Starts automatic timeout monitoring for stored requests.
     * 
     * <p>
     * The provided handler will be periodically invoked with any requests that have
     * exceeded their timeout duration. This prevents memory leaks from requests
     * that
     * never receive responses (e.g., due to application crashes or network issues).
     * 
     * @param timeoutHandler callback to invoke when requests time out
     */
    void startTimeoutMonitoring(TimeoutHandler timeoutHandler);

    /**
     * Stops the automatic timeout monitoring.
     * 
     * <p>
     * Should be called during shutdown to clean up background threads/tasks.
     */
    void stopTimeoutMonitoring();

    /**
     * Manually triggers timeout processing (for implementations that don't use
     * automatic monitoring).
     * 
     * <p>
     * This method can be called explicitly to check for and process any timed-out
     * requests.
     */
    void processTimeouts(); // For manual timeout processing

    /**
     * Closes the request store and releases any resources (connections, threads,
     * etc.).
     * 
     * <p>
     * Implementations should ensure graceful shutdown, flushing any pending data
     * and closing database connections or Redis clients.
     * 
     * @throws Exception if an error occurs during shutdown
     */
    void close() throws Exception;
}