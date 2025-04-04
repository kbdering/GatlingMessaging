package io.github.kbdering.kafka.cache;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryRequestStore implements RequestStore {
    private Map<String, Map<String, String>> cache;
    public final static String KEY = "key";
    public final static String VALUE = "value";
    public final static String TRANSACTION_NAME = "transactionName";
    public final static String START_TIME = "startTime";


    public InMemoryRequestStore() {
        cache = new ConcurrentHashMap<>();
    }
    @Override
    public void storeRequest(String correlationId, String key, String value, String transactionName, long startTime, long timeoutMillis) {
        cache.put(correlationId, Map.of(
                KEY, key,
                VALUE, null != value ? value : "value", // should we allow null values?
                TRANSACTION_NAME, transactionName,
                START_TIME, String.valueOf(startTime)
        ));
    }

    @Override
    public Map<String, String> getRequest(String correlationId) {
        return cache.get(correlationId);
    }

    @Override
    public Map<String, Map<String, String>> getRequests(List<String> correlationIds) {
        return null;
    }

    @Override
    public void deleteRequest(String correlationId) {
        cache.remove(correlationId);
    }

    @Override
    public void close() throws Exception {

    }
}
