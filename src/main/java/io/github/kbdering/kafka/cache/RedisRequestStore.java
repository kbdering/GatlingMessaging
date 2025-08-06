package io.github.kbdering.kafka.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import io.github.kbdering.kafka.SerializationType; // Import SerializationType

import java.util.Base64; // For Base64 encoding/decoding
import java.util.Collections;
import java.util.HashMap; // To construct the result map
import java.util.List;
import java.util.Map;


/*

dummy class for redis connections.
is postgres really more suitable for this?

 */
public class RedisRequestStore implements RequestStore {

    private final JedisPool jedisPool;

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
                // Corrected expiration: EXPIRE command works on keys, not fields within a hash.
                // We set the expiration on the entire hash (correlationId).
                // timeoutMillis is in milliseconds, jedis.expire takes seconds.
                jedis.expire(correlationId, (int) (timeoutMillis / 1000));
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
            jedis.del(correlationId); // Or hdel if you want to be more specific
        }
    }

    @Override
    public void close() {
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