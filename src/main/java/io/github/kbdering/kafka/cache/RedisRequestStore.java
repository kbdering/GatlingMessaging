package io.github.kbdering.kafka.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import io.github.kbdering.kafka.SerializationType; // Import SerializationType

import java.util.Base64; // For Base64 encoding/decoding
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
            requestData.put(InMemoryRequestStore.KEY, key);
            if (valueBytes != null) {
                requestData.put(InMemoryRequestStore.VALUE_BYTES, Base64.getEncoder().encodeToString(valueBytes));
            } else {
                requestData.put(InMemoryRequestStore.VALUE_BYTES, ""); // Store empty string for null bytes, or handle as needed
            }
            requestData.put(InMemoryRequestStore.SERIALIZATION_TYPE, serializationType.name());
            requestData.put(InMemoryRequestStore.TRANSACTION_NAME, transactionName);
            requestData.put(InMemoryRequestStore.START_TIME, String.valueOf(startTime));

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
            result.put(InMemoryRequestStore.KEY, storedData.get(InMemoryRequestStore.KEY));
            String base64ValueBytes = storedData.get(InMemoryRequestStore.VALUE_BYTES);
            if (base64ValueBytes != null && !base64ValueBytes.isEmpty()) {
                result.put(InMemoryRequestStore.VALUE_BYTES, Base64.getDecoder().decode(base64ValueBytes));
            }
            result.put(InMemoryRequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(storedData.get(InMemoryRequestStore.SERIALIZATION_TYPE)));
            result.put(InMemoryRequestStore.TRANSACTION_NAME, storedData.get(InMemoryRequestStore.TRANSACTION_NAME));
            result.put(InMemoryRequestStore.START_TIME, storedData.get(InMemoryRequestStore.START_TIME)); // Remains String as per other stores
            return result;
        }
    }

    @Override
    public Map<String, Map<String, Object>> getRequests(List<String> correlationIds) {
        // TODO: Implement batch get if needed. For now, returning null as per original.
        // This would involve iterating correlationIds, calling getRequest for each, and collecting results.
        return null;
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
}