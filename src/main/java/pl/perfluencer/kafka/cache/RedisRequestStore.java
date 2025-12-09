package pl.perfluencer.kafka.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import pl.perfluencer.kafka.util.SerializationType; // Import SerializationType

import java.util.ArrayList;
import java.util.Base64; // For Base64 encoding/decoding
import java.util.Collections;
import java.util.HashMap; // To construct the result map
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/*

dummy class for redis connections.
is postgres really more suitable for this?

 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisRequestStore implements RequestStore {

    private static final Logger logger = LoggerFactory.getLogger(RedisRequestStore.class);

    // Lua script to atomically get and delete multiple requests.
    // This is highly efficient as it's a single round-trip to Redis.
    // KEYS are the correlation IDs. ARGV[1] is the timeout sorted set key.
    private static final String PROCESS_BATCH_LUA = "local results = {}\n" +
            "local timeoutSetKey = ARGV[1]\n" +
            "for i, correlationId in ipairs(KEYS) do\n" +
            "  local requestData = redis.call('HGETALL', correlationId)\n" +
            "  if #requestData > 0 then\n" +
            "    redis.call('DEL', correlationId)\n" +
            "    redis.call('ZREM', timeoutSetKey, correlationId)\n" +
            "    table.insert(results, requestData)\n" +
            "  else\n" +
            "    table.insert(results, {})\n" +
            "  end\n" +
            "end\n" +
            "return results";

    // Lua script to atomically find, retrieve, and delete a batch of timed-out
    // requests.
    // This is much more efficient than fetching IDs and then processing them one by
    // one.
    // KEYS[1]: timeoutSetKey
    // ARGV[1]: currentTime
    // ARGV[2]: limit (how many to process at once)
    private static final String PROCESS_TIMEOUTS_LUA = "local timeoutSetKey = KEYS[1]\n" +
            "local currentTime = ARGV[1]\n" +
            "local limit = tonumber(ARGV[2])\n" +
            "local timedOutIds = redis.call('ZRANGEBYSCORE', timeoutSetKey, 0, currentTime, 'LIMIT', 0, limit)\n" +
            "if #timedOutIds == 0 then return {} end\n" +
            "redis.call('ZREM', timeoutSetKey, unpack(timedOutIds))\n" +
            "local allResults = {}\n" +
            "for i, correlationId in ipairs(timedOutIds) do\n" +
            "  local requestData = redis.call('HGETALL', correlationId)\n" +
            "  if #requestData > 0 then\n" +
            "    redis.call('DEL', correlationId)\n" +
            "    local result = {correlationId}\n" +
            "    for j, val in ipairs(requestData) do table.insert(result, val) end\n" +
            "    table.insert(allResults, result)\n" +
            "  end\n" +
            "end\n" +
            "return allResults";
    private final JedisPool jedisPool;
    private final String timeoutSetKey = "request_timeouts"; // Sorted set for timeout tracking
    private ScheduledExecutorService timeoutExecutor;
    private TimeoutHandler timeoutHandler;
    private final AtomicBoolean monitoringActive = new AtomicBoolean(false);

    public RedisRequestStore(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public void storeRequest(String correlationId, String key, Object value, SerializationType serializationType,
            String transactionName, String scenarioName, long startTime, long timeoutMillis) {
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
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
                requestData.put(RequestStore.VALUE_BYTES, ""); // Store empty string for null bytes, or handle as needed
            }
            requestData.put(RequestStore.SERIALIZATION_TYPE, serializationType.name());
            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
            requestData.put(RequestStore.SCENARIO_NAME, scenarioName);
            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));

            p.hmset(correlationId, requestData);

            if (timeoutMillis > 0) {
                // Set expiration on the hash. Round up to the nearest second to avoid premature
                // expiry.
                long expireSeconds = (timeoutMillis / 1000) + (timeoutMillis % 1000 == 0 ? 0 : 1);
                p.expire(correlationId, (int) expireSeconds);

                // Also add to timeout tracking sorted set for distributed timeout handling
                long timeoutTimestamp = startTime + timeoutMillis;
                p.zadd(timeoutSetKey, timeoutTimestamp, correlationId);
            }
            p.sync();
        } catch (JedisException e) {
            logger.error(
                    "Failed to connect to Redis. Potential Fix: Ensure Redis is running and accessible at the configured host:port.",
                    e);
            throw e;
        }
    }

    @Override
    public Map<String, Object> getRequest(String correlationId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> storedData = jedis.hgetAll(correlationId);
            if (storedData == null || storedData.isEmpty()) {
                return null;
            }

            return parseRedisHashMap(storedData);
        }
    }

    @Override
    public Map<String, Map<String, Object>> getRequests(List<String> correlationIds) {
        if (correlationIds == null || correlationIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Map<String, Object>> foundRequests = new HashMap<>();
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            Map<String, Response<Map<String, String>>> responses = new HashMap<>();
            for (String correlationId : correlationIds) {
                responses.put(correlationId, p.hgetAll(correlationId));
            }
            p.sync();

            for (Map.Entry<String, Response<Map<String, String>>> entry : responses.entrySet()) {
                String correlationId = entry.getKey();
                Map<String, String> storedData = entry.getValue().get();
                if (storedData != null && !storedData.isEmpty()) {
                    foundRequests.put(correlationId, parseRedisHashMap(storedData));
                }
            }
        }
        return foundRequests;
    }

    @Override
    public void deleteRequest(String correlationId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            p.del(correlationId);
            p.zrem(timeoutSetKey, correlationId);
            p.sync();
        }
    }

    @Override
    public void startTimeoutMonitoring(TimeoutHandler timeoutHandler) {
        this.timeoutHandler = timeoutHandler;
        if (monitoringActive.compareAndSet(false, true)) {
            timeoutExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "RedisRequestStore-TimeoutMonitor");
                t.setDaemon(true);
                return t;
            });
            // Check for timeouts every 10 seconds
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
    public void processTimeouts() {
        if (timeoutHandler == null) {
            return;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            long currentTime = System.currentTimeMillis();
            int limit = 1000; // Process up to 1000 timeouts at a time to avoid huge transactions

            @SuppressWarnings("unchecked")
            List<List<String>> results = (List<List<String>>) jedis.eval(
                    PROCESS_TIMEOUTS_LUA,
                    Collections.singletonList(timeoutSetKey),
                    List.of(String.valueOf(currentTime), String.valueOf(limit)));

            if (results != null && !results.isEmpty()) {
                for (List<String> result : results) {
                    if (result != null && !result.isEmpty()) {
                        String correlationId = result.get(0);
                        List<String> hashData = result.subList(1, result.size());
                        Map<String, Object> requestData = parseRedisList(hashData);
                        if (!requestData.isEmpty()) {
                            try {
                                timeoutHandler.onTimeout(correlationId, requestData);
                            } catch (Exception e) {
                                logger.error("Error processing timeout for correlationId {}: {}", correlationId,
                                        e.getMessage());
                            }
                        }
                    }
                }

            }
        } catch (JedisException e) {
            logger.error("Error processing timeouts in Redis: {}", e.getMessage());
        }
    }

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

    private Map<String, Object> parseRedisList(List<String> hashData) {
        Map<String, String> storedData = new HashMap<>();
        for (int i = 0; i < hashData.size(); i += 2) {
            if (i + 1 < hashData.size()) {
                storedData.put(hashData.get(i), hashData.get(i + 1));
            }
        }
        return parseRedisHashMap(storedData);
    }

    @Override
    public void close() {
        stopTimeoutMonitoring();
        jedisPool.close();
    }

    @Override
    public void processBatchedRecords(Map<String, Object> records, BatchProcessor process) {
        if (records == null || records.isEmpty()) {
            return;
        }

        Map<String, Map<String, Object>> foundRequests = new HashMap<>();
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> keys = new ArrayList<>(records.keySet());
            List<String> args = Collections.singletonList(timeoutSetKey);

            @SuppressWarnings("unchecked")
            // The result is a list of lists. Each inner list is the HGETALL result (a list
            // of strings) or an empty list.
            List<List<String>> results = (List<List<String>>) jedis.eval(PROCESS_BATCH_LUA, keys, args);

            // Correlate results back to original IDs
            for (int i = 0; i < keys.size(); i++) {
                String correlationId = keys.get(i);
                List<String> requestDataList = results.get(i);
                if (requestDataList != null && !requestDataList.isEmpty()) {
                    foundRequests.put(correlationId, parseRedisList(requestDataList));
                }
            }
        } catch (JedisException e) {
            // Log the error. The foundRequests map will be empty, so all records will be
            // treated as unmatched.
            logger.error("Error processing batched records from Redis: {}", e.getMessage());
        }

        // Process all records, distinguishing between matched (and now deleted) and
        // unmatched
        for (Map.Entry<String, Object> recordEntry : records.entrySet()) {
            String correlationId = recordEntry.getKey();
            if (foundRequests.containsKey(correlationId)) {
                process.onMatch(correlationId, foundRequests.get(correlationId), recordEntry.getValue());
            } else {
                process.onUnmatched(correlationId, recordEntry.getValue());
            }
        }
    }
}