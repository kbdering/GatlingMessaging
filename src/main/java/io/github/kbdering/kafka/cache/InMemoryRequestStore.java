package io.github.kbdering.kafka.cache;


import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.github.kbdering.kafka.SerializationType;

public class InMemoryRequestStore implements RequestStore {
    private final Map<String, Map<String, Object>> cache;

    public InMemoryRequestStore() {
        cache = new ConcurrentHashMap<>();
    }
    @Override
    public void storeRequest(String correlationId, String key, byte[] valueBytes, SerializationType serializationType, String transactionName, long startTime, long timeoutMillis) {
        // timeoutMillis is not used in InMemoryRequestStore, but kept for interface compatibility
        cache.put(correlationId, Map.of(
                RequestStore.KEY, key,
                RequestStore.VALUE_BYTES, valueBytes, // Store byte array
                RequestStore.SERIALIZATION_TYPE, serializationType, // Store serialization type
                RequestStore.TRANSACTION_NAME, transactionName,
                RequestStore.START_TIME, String.valueOf(startTime)
        ));
    }

    @Override
    public Map<String, Object> getRequest(String correlationId) {
        return cache.get(correlationId);
    }

    @Override
    public Map<String, Map<String, Object>> getRequests(List<String> correlationIds) {
        if (correlationIds == null || correlationIds.isEmpty()) {
            return Collections.emptyMap();
        }
        
        Map<String, Map<String, Object>> foundRequests = new HashMap<>();
        for (String correlationId : correlationIds) {
            Map<String, Object> requestData = cache.get(correlationId);
            if (requestData != null) {
                foundRequests.put(correlationId, requestData);
            }
        }
        return foundRequests;
    }

    @Override
    public void deleteRequest(String correlationId) {
        cache.remove(correlationId);
    }

    @Override
    public void close() throws Exception {

    }
    @Override
    public void processBatchedRecords(Map<String, byte[]> records, BatchProcessor process) {
        if (records == null || records.isEmpty()) {
            return;
        }
        
        // Process all records, distinguishing between matched (and now deleted) and unmatched
        for (Map.Entry<String, byte[]> recordEntry : records.entrySet()) {
            String correlationId = recordEntry.getKey();
            Map<String, Object> requestData = cache.remove(correlationId); // Get and remove atomically
            
            if (requestData != null) {
                process.onMatch(correlationId, requestData, recordEntry.getValue());
            } else {
                process.onUnmatched(correlationId, recordEntry.getValue());
            }
        }
    }
}
