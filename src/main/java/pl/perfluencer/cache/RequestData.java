package pl.perfluencer.cache;

import pl.perfluencer.common.util.SerializationType;
import java.util.Map;

/**
 * A lightweight POJO to hold request data in the buffering queue.
 * Using this instead of HashMap reduces allocation overhead and boxing
 * during the critical path (storeRequest).
 */
public class RequestData {
    public final String correlationId;
    public final String key;
    public final Object value;
    public final SerializationType serializationType;
    public final String transactionName;
    public final String scenarioName;
    public final long startTime;
    public final long timeoutMillis;
    public final Map<String, String> sessionVariables;

    public RequestData(String correlationId, String key, Object value, SerializationType serializationType,
            String transactionName, String scenarioName, long startTime, long timeoutMillis,
            Map<String, String> sessionVariables) {
        this.correlationId = correlationId;
        this.key = key;
        this.value = value;
        this.serializationType = serializationType;
        this.transactionName = transactionName;
        this.scenarioName = scenarioName;
        this.startTime = startTime;
        this.timeoutMillis = timeoutMillis;
        this.sessionVariables = sessionVariables;
    }

}
