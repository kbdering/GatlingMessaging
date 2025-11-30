package io.github.kbdering.kafka.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
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

    // Map of Timeout Duration -> Queue of Timeout Entries
    private final ConcurrentSkipListMap<Long, ConcurrentLinkedDeque<TimeoutEntry>> timeoutBuckets;

    // Helper class to track timeout entries
    private static class TimeoutEntry {
        final String correlationId;
        final long expirationTime;

        TimeoutEntry(String correlationId, long expirationTime) {
            this.correlationId = correlationId;
            this.expirationTime = expirationTime;
        }
    }

    private ScheduledExecutorService timeoutExecutor;
    private TimeoutHandler timeoutHandler;
    private final long timeoutCheckIntervalMillis;

    public InMemoryRequestStore() {
        this(5000);
    }

    public InMemoryRequestStore(long timeoutCheckIntervalMillis) {
        this.timeoutCheckIntervalMillis = timeoutCheckIntervalMillis;
        this.timeoutBuckets = new ConcurrentSkipListMap<>();
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
            long expirationTime = startTime + timeoutMillis;

            // Add to the bucket for this specific timeout duration
            timeoutBuckets.computeIfAbsent(timeoutMillis, k -> new ConcurrentLinkedDeque<>())
                    .add(new TimeoutEntry(correlationId, expirationTime));
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

    // Not an override from RequestStore, but used internally and by tests
    public Map<String, Object> remove(String correlationId) {
        // Lazy removal: we only remove from cache.
        // The timeout entry will be cleaned up when it expires and we check the cache.
        return cache.remove(correlationId);
    }

    @Override
    public void deleteRequest(String correlationId) {
        remove(correlationId);
    }

    @Override
    public void startTimeoutMonitoring(TimeoutHandler handler) {
        this.timeoutHandler = handler;
        if (timeoutExecutor == null || timeoutExecutor.isShutdown()) {
            timeoutExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "TimeoutMonitor");
                t.setDaemon(true);
                return t;
            });
            timeoutExecutor.scheduleAtFixedRate(this::processTimeouts, timeoutCheckIntervalMillis,
                    timeoutCheckIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stopTimeoutMonitoring() {
        if (timeoutExecutor != null) {
            timeoutExecutor.shutdown();
            try {
                if (!timeoutExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                    timeoutExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                timeoutExecutor.shutdownNow();
            }
        }
    }

    @Override
    public void processTimeouts() {
        try {
            if (timeoutHandler == null) {
                return;
            }

            long currentTime = System.currentTimeMillis();

            // Iterate over all timeout buckets (durations)
            for (Map.Entry<Long, ConcurrentLinkedDeque<TimeoutEntry>> entry : timeoutBuckets.entrySet()) {
                ConcurrentLinkedDeque<TimeoutEntry> queue = entry.getValue();

                // Process the queue for this duration
                while (true) {
                    TimeoutEntry head = queue.peek();

                    // If queue is empty or head has not expired, stop processing this bucket
                    if (head == null || head.expirationTime > currentTime) {
                        break;
                    }

                    // Remove the expired entry from the queue
                    queue.poll();

                    // Check if the request is still active (lazy removal check)
                    Map<String, Object> requestData = cache.remove(head.correlationId);

                    if (requestData != null) {
                        // Request was still in cache, so it's a real timeout
                        try {
                            timeoutHandler.onTimeout(head.correlationId, requestData);
                        } catch (Exception e) {
                            logger.error("Error processing timeout for correlationId {}: {}", head.correlationId,
                                    e.getMessage());
                        }
                    }
                    // If requestData is null, it means the response was already received and
                    // removed from cache
                }
            }
        } catch (Exception e) {
            logger.error("Error in processTimeouts", e);
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
