package pl.perfluencer.cache;

import java.util.Map;

public interface BatchProcessor {

    /**
     * Optimized callback for high-performance retrieval.
     * Implement this to avoid Map allocations.
     * Default implementation delegates to map-based dictionary for backward
     * compatibility.
     */
    void onMatch(String correlationId, RequestData requestData, Object responseValue);

    void onUnmatched(String correlationId, Object responseValue);

}
