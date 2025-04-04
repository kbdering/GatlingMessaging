package io.github.kbdering.kafka.cache;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;

public class RedisRequestStore implements RequestStore {

    private final JedisPool jedisPool;

    public RedisRequestStore(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public void storeRequest(String correlationId, String key, String value, String transactionName, long startTime, long timeoutMillis) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(correlationId, Map.of("key", key, "value", value, "startTime", String.valueOf(startTime), "transactionName", transactionName));
            jedis.hexpire(correlationId, timeoutMillis/ 1000, "value");

        }
    }


    @Override
    public Map<String, String> getRequest(String correlationId) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hgetAll(correlationId);
        }
    }

    @Override
    public Map<String, Map<String, String>> getRequests(List<String> correlationIds) {
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