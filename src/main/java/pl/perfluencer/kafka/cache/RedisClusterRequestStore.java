package pl.perfluencer.kafka.cache;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisException;
import pl.perfluencer.kafka.util.SerializationType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of RequestStore for Redis Cluster.
 * <p>
 * Unlike {@link RedisRequestStore} which uses pipelines for efficiency on a
 * single node,
 * this implementation uses direct commands because atomic Multi-Key operations
 * (pipelines, transactions, Lua)
 * are restricted to the same Hash Slot in Redis Cluster.
 * <p>
 * This implementation accepts the trade-off of higher network round-trips for
 * the ability to scale horizontally.
 */
public class RedisClusterRequestStore implements RequestStore {

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterRequestStore.class);

    private final JedisCluster jedisCluster;
    private static final int TIMEOUT_SHARD_COUNT = 64;
    private static final String TIMEOUT_SET_KEY_PREFIX = "request_timeouts:{%d}";

    private String getTimeoutSetKey(String correlationId) {
        int shard = Math.abs(correlationId.hashCode()) % TIMEOUT_SHARD_COUNT;
        return String.format(TIMEOUT_SET_KEY_PREFIX, shard);
    }

    private ScheduledExecutorService timeoutExecutor;
    private TimeoutHandler timeoutHandler;
    private final AtomicBoolean monitoringActive = new AtomicBoolean(false);

    public RedisClusterRequestStore(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void storeRequest(String correlationId, String key, Object value, SerializationType serializationType,
            String transactionName, String scenarioName, long startTime, long timeoutMillis) {
        try {
            // WE CANNOT use pipeline here comfortably across slots.
            // correlationId is the hash key.

            Map<String, String> requestData = new HashMap<>();
            requestData.put(RequestStore.KEY, key);
            if (value != null) {
                byte[] bytes;
                if (value instanceof byte[]) {
                    bytes = (byte[]) value;
                } else if (value instanceof String) {
                    bytes = ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                } else {
                    bytes = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                requestData.put(RequestStore.VALUE_BYTES, Base64.getEncoder().encodeToString(bytes));
            } else {
                requestData.put(RequestStore.VALUE_BYTES, "");
            }
            requestData.put(RequestStore.SERIALIZATION_TYPE, serializationType.name());
            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));

            // 1. Store the Hash (Cluster routing handles this based on correlationId)
            jedisCluster.hmset(correlationId, requestData);

            if (timeoutMillis > 0) {
                // 2. Set Expiry on the Hash
                long expireSeconds = (timeoutMillis / 1000) + (timeoutMillis % 1000 == 0 ? 0 : 1);
                jedisCluster.expire(correlationId, (int) expireSeconds);

                // 3. Add to Timeout Sorted Set (Shard-aware)
                // This distributes the timeout writes across the cluster based on correlationId
                // hash.
                String timeoutKey = getTimeoutSetKey(correlationId);
                long timeoutTimestamp = startTime + timeoutMillis;
                jedisCluster.zadd(timeoutKey, timeoutTimestamp, correlationId);
            }

        } catch (JedisException e) {
            logger.error("Failed to store request in Redis Cluster.", e);
            throw e;
        }
    }

    @Override
    public Map<String, Object> getRequest(String correlationId) {
        try {
            // Direct lookup
            Map<String, String> storedData = jedisCluster.hgetAll(correlationId);
            if (storedData == null || storedData.isEmpty()) {
                return null;
            }
            return parseRedisHashMap(storedData);
        } catch (JedisException e) {
            logger.error("Failed to get request from Redis Cluster.", e);
            throw e;
        }
    }

    @Override
    public Map<String, Map<String, Object>> getRequests(List<String> correlationIds) {
        if (correlationIds == null || correlationIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Map<String, Object>> foundRequests = new HashMap<>();

        // Cannot easy pipeline across slots. Iterate.
        // Parallel stream could speed this up if needed, but keeping it simple for now.
        for (String correlationId : correlationIds) {
            Map<String, String> storedData = jedisCluster.hgetAll(correlationId);
            if (storedData != null && !storedData.isEmpty()) {
                foundRequests.put(correlationId, parseRedisHashMap(storedData));
            }
        }
        return foundRequests;
    }

    @Override
    public void deleteRequest(String correlationId) {
        try {
            // 2 calls, potentially different slots
            jedisCluster.del(correlationId);
            String timeoutKey = getTimeoutSetKey(correlationId);
            jedisCluster.zrem(timeoutKey, correlationId);
        } catch (JedisException e) {
            logger.error("Error deleting request from Redis Cluster", e);
        }
    }

    @Override
    public void processBatchedRecords(Map<String, Object> records, BatchProcessor process) {
        if (records == null || records.isEmpty()) {
            return;
        }

        // Optimizing this for cluster is hard without grouping by slot.
        // We will just iterate.
        // Note: This is an "O(N) network calls" approach for N records.
        // In Cluster, to optimize, one might use Jedis's advanced pipelining if
        // available,
        // or accept this trade-off.

        for (Map.Entry<String, Object> recordEntry : records.entrySet()) {
            String correlationId = recordEntry.getKey();
            Map<String, Object> requestData = getRequest(correlationId);

            if (requestData != null) {
                // If found, delete it (emulating atomic "get and delete" non-atomically)
                deleteRequest(correlationId); // 2 more network calls
                process.onMatch(correlationId, requestData, recordEntry.getValue());
            } else {
                process.onUnmatched(correlationId, recordEntry.getValue());
            }
        }
    }

    @Override
    public void processTimeouts() {
        if (timeoutHandler == null) {
            return;
        }

        try {
            long currentTime = System.currentTimeMillis();

            // Iterate all shards to find timeouts.
            // This spreads the read load.
            for (int i = 0; i < TIMEOUT_SHARD_COUNT; i++) {
                String timeoutKey = String.format(TIMEOUT_SET_KEY_PREFIX, i);

                // 1. Get timed out IDs from this shard
                // Limit to 100 per shard to keep total reasonable (100 * 64 = 6400 potential
                // timeouts processed)
                List<String> timedOutIds = jedisCluster.zrangeByScore(timeoutKey, 0, currentTime, 0, 100);

                if (timedOutIds != null && !timedOutIds.isEmpty()) {

                    // 2. Remove them from the set so we don't process them again (optimistic
                    // locking)
                    jedisCluster.zrem(timeoutKey, timedOutIds.toArray(new String[0]));

                    // 3. Fetch details for each (Scatter-Gather across cluster)
                    for (String correlationId : timedOutIds) {
                        Map<String, String> hashData = jedisCluster.hgetAll(correlationId);

                        if (hashData != null && !hashData.isEmpty()) {
                            // Delete the hash
                            jedisCluster.del(correlationId);

                            Map<String, Object> requestData = parseRedisHashMap(hashData);
                            try {
                                timeoutHandler.onTimeout(correlationId, requestData);
                            } catch (Exception e) {
                                logger.error("Error processing timeout handler for {}", correlationId, e);
                            }
                        }
                    }
                }
            }
        } catch (JedisException e) {
            logger.error("Error processing timeouts in Redis Cluster: {}", e.getMessage());
        }
    }

    // Helper methods identical to RedisRequestStore
    private Map<String, Object> parseRedisHashMap(Map<String, String> storedData) {
        Map<String, Object> result = new HashMap<>();
        result.put(RequestStore.KEY, storedData.get(RequestStore.KEY));
        String base64ValueBytes = storedData.get(RequestStore.VALUE_BYTES);
        if (base64ValueBytes != null && !base64ValueBytes.isEmpty()) {
            result.put(RequestStore.VALUE_BYTES, Base64.getDecoder().decode(base64ValueBytes));
        }
        result.put(RequestStore.SERIALIZATION_TYPE,
                SerializationType.valueOf(storedData.get(RequestStore.SERIALIZATION_TYPE)));
        result.put(RequestStore.TRANSACTION_NAME, storedData.get(RequestStore.TRANSACTION_NAME));
        result.put(RequestStore.SCENARIO_NAME, storedData.get(RequestStore.SCENARIO_NAME));
        result.put(RequestStore.START_TIME, storedData.get(RequestStore.START_TIME));
        return result;
    }

    @Override
    public void startTimeoutMonitoring(TimeoutHandler timeoutHandler) {
        this.timeoutHandler = timeoutHandler;
        if (monitoringActive.compareAndSet(false, true)) {
            timeoutExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "RedisCluster-TimeoutMonitor");
                t.setDaemon(true);
                return t;
            });
            timeoutExecutor.scheduleWithFixedDelay(this::processTimeouts, 10, 10, TimeUnit.SECONDS);
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
    public void close() throws Exception {
        stopTimeoutMonitoring();
        jedisCluster.close();
    }
}
