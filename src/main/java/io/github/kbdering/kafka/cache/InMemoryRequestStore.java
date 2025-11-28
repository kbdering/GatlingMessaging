package io.github.kbdering.kafka.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import io.github.kbdering.kafka.util.SerializationType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryRequestStore implements RequestStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryRequestStore.class);
    private final ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
    // Sorted map: timeout timestamp -> set of correlation IDs that timeout at that
    // time
    private final ConcurrentSkipListMap<Long, Set<String>> timeoutsByTime;
    // Quick lookup: correlation ID -> timeout timestamp
    private final Map<String, Long> correlationToTimeout;
    private ScheduledExecutorService timeoutExecutor;
    private TimeoutHandler timeoutHandler;
    private final AtomicBoolean monitoringActive = new AtomicBoolean(false);

    public InMemoryRequestStore() {
        // cache initialized inline
        timeoutsByTime = new ConcurrentSkipListMap<>();
        correlationToTimeout = new ConcurrentHashMap<>();
    }

    @Override
    public void storeRequest(String correlationId, String key, Object value, SerializationType serializationType,
            String transactionName, String scenarioName, long startTime, long timeoutMillis) {
        Map<String, Object> requestData = new ConcurrentHashMap<>();
        if (key != null) {
            requestData.put(RequestStore.KEY, key);
        }
        if (value != null) {
            requestData.put(RequestStore.VALUE_BYTES, value);
        }
        if (serializationType != null) {
            requestData.put(RequestStore.SERIALIZATION_TYPE, serializationType);
        }
        requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
        requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
        requestData.put(RequestStore.START_TIME, String.valueOf(startTime));
        cache.put(correlationId, requestData);

        // Store timeout information if timeout is specified
        if (timeoutMillis > 0) {
            long timeoutTimestamp = startTime + timeoutMillis;
            correlationToTimeout.put(correlationId, timeoutTimestamp);

            // Add to sorted timeout structure
            timeoutsByTime.computeIfAbsent(timeoutTimestamp, k -> new ConcurrentSkipListSet<>())
                    .add(correlationId);
        }
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

        // Remove from timeout tracking
        Long timeoutTimestamp = correlationToTimeout.remove(correlationId);
        if (timeoutTimestamp != null) {
            Set<String> timeoutSet = timeoutsByTime.get(timeoutTimestamp);
            if (timeoutSet != null) {
                timeoutSet.remove(correlationId);
                // Clean up empty timeout buckets
                if (timeoutSet.isEmpty()) {
                    timeoutsByTime.remove(timeoutTimestamp);
                }
            }
        }
    }

    @Override
    public void startTimeoutMonitoring(TimeoutHandler timeoutHandler) {
        this.timeoutHandler = timeoutHandler;
        if (monitoringActive.compareAndSet(false, true)) {
            timeoutExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "InMemoryRequestStore-TimeoutMonitor");
                t.setDaemon(true);
                return t;
            });
            // Check for timeouts every 5 seconds
            timeoutExecutor.scheduleWithFixedDelay(this::processTimeouts, 5, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void stopTimeoutMonitoring() {
        if (monitoringActive.compareAndSet(true, false)) {
            if (timeoutExecutor != null) {
                timeoutExecutor.shutdown();
                try {
                    if (!timeoutExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        timeoutExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    timeoutExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                timeoutExecutor = null;
            }
        }
        this.timeoutHandler = null;
    }

    @Override
    public void processTimeouts() {
        if (timeoutHandler == null) {
            return;
        }

        long currentTime = System.currentTimeMillis();

        // Get all timeout timestamps that have expired (O(log n) operation)
        Map<Long, Set<String>> expiredTimeouts = timeoutsByTime.headMap(currentTime, true);

        if (expiredTimeouts.isEmpty()) {
            return;
        }

        // Process all expired timeouts
        List<Long> timestampsToRemove = new ArrayList<>();

        for (Map.Entry<Long, Set<String>> entry : expiredTimeouts.entrySet()) {
            Long timeoutTimestamp = entry.getKey();
            Set<String> correlationIds = entry.getValue();

            // Process each correlation ID in this timeout bucket
            for (String correlationId : correlationIds) {
                Map<String, Object> requestData = cache.remove(correlationId);
                correlationToTimeout.remove(correlationId);

                if (requestData != null) {
                    try {
                        timeoutHandler.onTimeout(correlationId, requestData);
                    } catch (Exception e) {
                        logger.error("Error processing timeout for correlationId {}: {}", correlationId,
                                e.getMessage());
                    }
                }
            }

            timestampsToRemove.add(timeoutTimestamp);
        }

        // Clean up processed timeout buckets
        for (Long timestamp : timestampsToRemove) {
            timeoutsByTime.remove(timestamp);
        }
    }

    @Override
    public void close() throws Exception {
        stopTimeoutMonitoring();
    }

    @Override
    public void processBatchedRecords(Map<String, Object> records, BatchProcessor process) {
        if (records == null || records.isEmpty()) {
            return;
        }

        // Process all records, distinguishing between matched (and now deleted) and
        // unmatched
        for (Map.Entry<String, Object> recordEntry : records.entrySet()) {
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
