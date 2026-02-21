/*
 * Copyright 2026 Perfluencer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.perfluencer.cache;

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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import pl.perfluencer.common.util.SerializationType;
import pl.perfluencer.cache.config.CoreStoreConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryRequestStore implements RequestStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryRequestStore.class);
    // Changed to store RequestData directly
    private final ConcurrentHashMap<String, RequestData> cache = new ConcurrentHashMap<>();

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

    private final CoreStoreConfig config;

    public InMemoryRequestStore() {
        this(new CoreStoreConfig());
    }

    public InMemoryRequestStore(long timeoutCheckIntervalMillis) {
        this(new CoreStoreConfig().timeoutCheckInterval(Duration.ofMillis(timeoutCheckIntervalMillis)));
    }

    public InMemoryRequestStore(CoreStoreConfig config) {
        this.config = config != null ? config : new CoreStoreConfig();
        this.timeoutCheckIntervalMillis = this.config.getTimeoutCheckInterval().toMillis();
        this.timeoutBuckets = new ConcurrentSkipListMap<>();
    }

    @Override
    public void storeRequest(RequestData requestData) {
        cache.put(requestData.correlationId, requestData);

        // Store timeout information if timeout is specified
        if (requestData.timeoutMillis > 0) {
            long expirationTime = requestData.startTime + requestData.timeoutMillis;

            // Add to the bucket for this specific timeout duration
            timeoutBuckets.computeIfAbsent(requestData.timeoutMillis, k -> new ConcurrentLinkedDeque<>())
                    .add(new TimeoutEntry(requestData.correlationId, expirationTime));
        }
    }

    @Override
    public void storeRequestBatch(List<RequestData> requests) {
        for (RequestData request : requests) {
            storeRequest(request);
        }
    }

    @Override
    public RequestData getRequest(String correlationId) {
        return cache.get(correlationId);
    }

    @Override
    public Map<String, RequestData> getRequests(List<String> correlationIds) {
        if (correlationIds == null || correlationIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, RequestData> foundRequests = new HashMap<>();
        for (String correlationId : correlationIds) {
            RequestData data = cache.get(correlationId);
            if (data != null) {
                foundRequests.put(correlationId, data);
            }
        }
        return foundRequests;
    }

    // Not an override from RequestStore, but used internally and by tests
    public RequestData remove(String correlationId) {
        return cache.remove(correlationId);
    }

    @Override
    public void deleteRequest(String correlationId) {
        cache.remove(correlationId);
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
                    RequestData requestData = cache.remove(head.correlationId);

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
            RequestData requestData = cache.remove(correlationId); // Get and remove atomically

            if (requestData != null) {
                // Use optimized callback with RequestData POJO
                process.onMatch(correlationId, requestData, recordEntry.getValue());
            } else {
                process.onUnmatched(correlationId, recordEntry.getValue());
            }
        }
    }
}
