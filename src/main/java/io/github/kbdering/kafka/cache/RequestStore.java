package io.github.kbdering.kafka.cache;

import java.util.List;
import java.util.Map;
import io.github.kbdering.kafka.util.SerializationType;

public interface RequestStore extends AutoCloseable {
    String KEY = "key";
    String VALUE_BYTES = "valueBytes";
    String SERIALIZATION_TYPE = "serializationType";
    String TRANSACTION_NAME = "transactionName";
    String SCENARIO_NAME = "scenarioName";
    String START_TIME = "startTime";

    void storeRequest(String correlationId, String key, Object value, SerializationType serializationType,
            String transactionName, String scenarioName, long startTime, long timeoutMillis);

    Map<String, Object> getRequest(String correlationId); // Value will be byte[], type will be SerializationType

    Map<String, Map<String, Object>> getRequests(List<String> correlationIds);

    void processBatchedRecords(Map<String, Object> records, BatchProcessor process);

    void deleteRequest(String correlationId);

    // Timeout handling methods
    void startTimeoutMonitoring(TimeoutHandler timeoutHandler);

    void stopTimeoutMonitoring();

    void processTimeouts(); // For manual timeout processing

    void close() throws Exception;
}