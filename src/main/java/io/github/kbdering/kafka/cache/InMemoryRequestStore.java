package io.github.kbdering.kafka.cache;


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
        // Implementation for batch get - for simplicity, returning null as in original
        // A proper implementation would iterate IDs and collect from cache.
        return null; 
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
        
        throw new UnsupportedOperationException("Unimplemented method 'processBatchedRecords'");
    }
}
