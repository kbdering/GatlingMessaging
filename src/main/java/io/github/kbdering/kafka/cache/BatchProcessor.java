package io.github.kbdering.kafka.cache;

import java.util.Map;

public interface BatchProcessor {
    void onMatch(String correlationId, Map<String, Object> requestData, Object responseValue);

    void onUnmatched(String correlationId, Object responseValue);

}
