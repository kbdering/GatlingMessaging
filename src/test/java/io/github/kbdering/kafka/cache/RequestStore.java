package io.github.kbdering.kafka.cache;
import java.util.List;
import java.util.Map;

public interface RequestStore extends AutoCloseable {
    void storeRequest(String correlationId, String key, String value, String transactionName, long startTime, long timeoutMillis);
    Map<String, String> getRequest(String correlationId);
    Map<String, Map<String, String>> getRequests(List<String> correlationIds);
    void deleteRequest(String correlationId);
    void close() throws Exception;
}