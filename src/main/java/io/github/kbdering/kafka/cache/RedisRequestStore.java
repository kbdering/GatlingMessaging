package io.github.kbdering.kafka.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import io.github.kbdering.kafka.SerializationType; // Import SerializationType

import java.util.Base64; // For Base64 encoding/decoding
import java.util.Collections;
import java.util.HashMap; // To construct the result map
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/*

dummy class for redis connections.
is postgres really more suitable for this?

 */
public class RedisRequestStore implements RequestStore {

    private final JedisPool jedisPool;
    private final String timeoutSetKey = "request_timeouts"; // Sorted set for timeout tracking
    private ScheduledExecutorService timeoutExecutor;
    private TimeoutHandler timeoutHandler;
    private final AtomicBoolean monitoringActive = new AtomicBoolean(false);

    public RedisRequestStore(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public void storeRequest(String correlationId, String key, byte[] valueBytes, SerializationType serializationType, String transactionName, long startTime, long timeoutMillis) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> requestData = new HashMap<>();
            requestData.put(RequestStore.KEY, key);
            if (valueBytes != null) {
                requestData.put(RequestStore.VALUE_BYTES, Base64.getEncoder().encodeToString(valueBytes));
            } else {
                requestData.put(RequestStore.VALUE_BYTES, ""); // Store empty string for null bytes, or handle as needed
            }
            requestData.put(RequestStore.SERIALIZATION_TYPE, serializationType.name());
            requestData.put(RequestStore.TRANSACTION_NAME, transactionName);
            requestData.put(RequestStore.START_TIME, String.valueOf(startTime));

            jedis.hmset(correlationId, requestData);

            if (timeoutMillis > 0) {
                // Set expiration on the hash
                jedis.expire(correlationId, (int) (timeoutMillis / 1000));
                
                // Also add to timeout tracking sorted set for distributed timeout handling
                long timeoutTimestamp = startTime + timeoutMillis;
                jedis.zadd(timeoutSetKey, timeoutTimestamp, correlationId);
            }
        }
    }


    @Override
    public Map<String, Object> getRequest(String correlationId) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> storedData = jedis.hgetAll(correlationId);
            if (storedData == null || storedData.isEmpty()) {
                return null;
            }

            Map<String, Object> result = new HashMap<>();
            result.put(RequestStore.KEY, storedData.get(RequestStore.KEY));
            String base64ValueBytes = storedData.get(RequestStore.VALUE_BYTES);
            if (base64ValueBytes != null && !base64ValueBytes.isEmpty()) {
                result.put(RequestStore.VALUE_BYTES, Base64.getDecoder().decode(base64ValueBytes));
            }
            result.put(RequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(storedData.get(RequestStore.SERIALIZATION_TYPE)));
            result.put(RequestStore.TRANSACTION_NAME, storedData.get(RequestStore.TRANSACTION_NAME));
            result.put(RequestStore.START_TIME, storedData.get(RequestStore.START_TIME)); // Remains String as per other stores
            return result;
        }
    }

    @Override
    public Map<String, Map<String, Object>> getRequests(List<String> correlationIds) {
        if (correlationIds == null || correlationIds.isEmpty()) {
            return Collections.emptyMap();
        }
        
        Map<String, Map<String, Object>> foundRequests = new HashMap<>();
        try (Jedis jedis = jedisPool.getResource()) {
            for (String correlationId : correlationIds) {
                Map<String, String> storedData = jedis.hgetAll(correlationId);
                if (storedData != null && !storedData.isEmpty()) {
                    Map<String, Object> result = new HashMap<>();
                    result.put(RequestStore.KEY, storedData.get(RequestStore.KEY));
                    String base64ValueBytes = storedData.get(RequestStore.VALUE_BYTES);
                    if (base64ValueBytes != null && !base64ValueBytes.isEmpty()) {
                        result.put(RequestStore.VALUE_BYTES, Base64.getDecoder().decode(base64ValueBytes));
                    }
                    result.put(RequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(storedData.get(RequestStore.SERIALIZATION_TYPE)));
                    result.put(RequestStore.TRANSACTION_NAME, storedData.get(RequestStore.TRANSACTION_NAME));
                    result.put(RequestStore.START_TIME, storedData.get(RequestStore.START_TIME));
                    foundRequests.put(correlationId, result);
                }
            }
        }
        return foundRequests;
    }

    @Override
    public void deleteRequest(String correlationId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(correlationId);
            // Remove from timeout tracking set
            jedis.zrem(timeoutSetKey, correlationId);
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
            
            // Get timed out requests using sorted set range
            Set<String> timedOutIds = new HashSet<>(jedis.zrangeByScore(timeoutSetKey, 0, currentTime));
            
            if (!timedOutIds.isEmpty()) {
                // Process each timed out request individually to ensure distributed safety
                for (String correlationId : timedOutIds) {
                    // Use Lua script to atomically check, retrieve, and delete timed out request
                    // This ensures only one instance processes each timeout in distributed environment
                    String luaScript = 
                        "local correlationId = ARGV[1]\n" +
                        "-- Try to remove from timeout set first (atomic operation)\n" +
                        "local removed = redis.call('ZREM', KEYS[1], correlationId)\n" +
                        "if removed == 1 then\n" +
                        "  -- Only process if we successfully removed it (ensures single processing)\n" +
                        "  local requestData = redis.call('HGETALL', correlationId)\n" +
                        "  if #requestData > 0 then\n" +
                        "    redis.call('DEL', correlationId)\n" +
                        "    return requestData\n" +
                        "  end\n" +
                        "end\n" +
                        "return {}";
                    
                    @SuppressWarnings("unchecked")
                    List<String> requestDataList = (List<String>) jedis.eval(luaScript, 1, timeoutSetKey, correlationId);
                    
                    if (!requestDataList.isEmpty()) {
                        processTimeoutResult(correlationId, requestDataList);
                    }
                }
                

            }
        } catch (Exception e) {
            System.err.println("Error processing timeouts in Redis: " + e.getMessage());
        }
    }
    
    private void processTimeoutResult(String correlationId, List<String> hashData) {
        Map<String, Object> requestData = new HashMap<>();
        
        // Parse Redis hash data (key-value pairs)
        for (int i = 0; i < hashData.size(); i += 2) {
            if (i + 1 < hashData.size()) {
                String key = hashData.get(i);
                String value = hashData.get(i + 1);
                
                if (RequestStore.KEY.equals(key)) {
                    requestData.put(RequestStore.KEY, value);
                } else if (RequestStore.VALUE_BYTES.equals(key)) {
                    if (value != null && !value.isEmpty()) {
                        requestData.put(RequestStore.VALUE_BYTES, Base64.getDecoder().decode(value));
                    }
                } else if (RequestStore.SERIALIZATION_TYPE.equals(key)) {
                    requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(value));
                } else if (RequestStore.TRANSACTION_NAME.equals(key)) {
                    requestData.put(RequestStore.TRANSACTION_NAME, value);
                } else if (RequestStore.START_TIME.equals(key)) {
                    requestData.put(RequestStore.START_TIME, value);
                }
            }
        }
        
        // Process timeout if we have data
        if (!requestData.isEmpty()) {
            try {
                timeoutHandler.onTimeout(correlationId, requestData);
            } catch (Exception e) {
                System.err.println("Error processing timeout for correlationId " + correlationId + ": " + e.getMessage());
            }
        }
    }
    
    // Keep the old method for compatibility but make it unused
    @SuppressWarnings("unused")
    private void processTimeoutResults(List<String> results) {
        if (results.isEmpty()) {
            return;
        }
        
        int i = 0;
        while (i < results.size()) {
            if (i >= results.size()) break;
            
            String correlationId = results.get(i++);
            Map<String, Object> requestData = new HashMap<>();
            
            // Parse hash data until we hit the separator
            while (i < results.size() && !"---".equals(results.get(i))) {
                if (i + 1 < results.size()) {
                    String key = results.get(i++);
                    String value = results.get(i++);
                    
                    if (RequestStore.KEY.equals(key)) {
                        requestData.put(RequestStore.KEY, value);
                    } else if (RequestStore.VALUE_BYTES.equals(key)) {
                        if (value != null && !value.isEmpty()) {
                            requestData.put(RequestStore.VALUE_BYTES, Base64.getDecoder().decode(value));
                        }
                    } else if (RequestStore.SERIALIZATION_TYPE.equals(key)) {
                        requestData.put(RequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(value));
                    } else if (RequestStore.TRANSACTION_NAME.equals(key)) {
                        requestData.put(RequestStore.TRANSACTION_NAME, value);
                    } else if (RequestStore.START_TIME.equals(key)) {
                        requestData.put(RequestStore.START_TIME, value);
                    }
                } else {
                    break;
                }
            }
            
            // Skip separator
            if (i < results.size() && "---".equals(results.get(i))) {
                i++;
            }
            
            // Process timeout if we have data
            if (!requestData.isEmpty()) {
                try {
                    timeoutHandler.onTimeout(correlationId, requestData);
                } catch (Exception e) {
                    System.err.println("Error processing timeout for correlationId " + correlationId + ": " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void close() {
        stopTimeoutMonitoring();
        jedisPool.close();
    }

    @Override
    public void processBatchedRecords(Map<String, byte[]> records, BatchProcessor process) {
        if (records == null || records.isEmpty()) {
            return;
        }
        
        try (Jedis jedis = jedisPool.getResource()) {
            // First, get all the request data for matching correlation IDs
            Map<String, Map<String, Object>> foundRequests = new HashMap<>();
            
            for (String correlationId : records.keySet()) {
                Map<String, String> storedData = jedis.hgetAll(correlationId);
                if (storedData != null && !storedData.isEmpty()) {
                    Map<String, Object> result = new HashMap<>();
                    result.put(RequestStore.KEY, storedData.get(RequestStore.KEY));
                    String base64ValueBytes = storedData.get(RequestStore.VALUE_BYTES);
                    if (base64ValueBytes != null && !base64ValueBytes.isEmpty()) {
                        result.put(RequestStore.VALUE_BYTES, Base64.getDecoder().decode(base64ValueBytes));
                    }
                    result.put(RequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(storedData.get(RequestStore.SERIALIZATION_TYPE)));
                    result.put(RequestStore.TRANSACTION_NAME, storedData.get(RequestStore.TRANSACTION_NAME));
                    result.put(RequestStore.START_TIME, storedData.get(RequestStore.START_TIME));
                    foundRequests.put(correlationId, result);
                    
                    // Delete the request after retrieving it
                    jedis.del(correlationId);
                }
            }
            
            // Process all records, distinguishing between matched (and now deleted) and unmatched
            for (Map.Entry<String, byte[]> recordEntry : records.entrySet()) {
                String correlationId = recordEntry.getKey();
                if (foundRequests.containsKey(correlationId)) {
                    process.onMatch(correlationId, foundRequests.get(correlationId), recordEntry.getValue());
                } else {
                    process.onUnmatched(correlationId, recordEntry.getValue());
                }
            }
        }
    }
}