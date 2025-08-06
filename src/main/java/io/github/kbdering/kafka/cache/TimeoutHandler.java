package io.github.kbdering.kafka.cache;

import java.util.Map;

/**
 * Interface for handling timeout events for requests that have exceeded their timeout period.
 */
public interface TimeoutHandler {
    /**
     * Called when a request has timed out.
     * 
     * @param correlationId The correlation ID of the timed out request
     * @param requestData The original request data that timed out
     */
    void onTimeout(String correlationId, Map<String, Object> requestData);
}
