package io.github.kbdering.kafka.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import io.github.kbdering.kafka.util.SerializationType;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
public class RedisRequestStoreTest {

    @Container
    public static GenericContainer<?> redis = new GenericContainer<>("redis:5.0.3-alpine")
            .withExposedPorts(6379);

    private JedisPool jedisPool;
    private RedisRequestStore store;

    @BeforeEach
    public void setUp() {
        String address = redis.getHost();
        Integer port = redis.getFirstMappedPort();
        jedisPool = new JedisPool(address, port);
        store = new RedisRequestStore(jedisPool);
    }

    @AfterEach
    public void tearDown() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    public void testStoreAndGetRequest() {
        String correlationId = "corr-1";
        String key = "key-1";
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        long startTime = 1000L;
        long timeout = 5000L;
        String transactionName = "test-req";
        String scenarioName = "test-scenario";

        store.storeRequest(correlationId, key, value, SerializationType.STRING,
                transactionName, scenarioName,
                startTime, timeout);

        Map<String, Object> retrieved = store.getRequest(correlationId);
        assertNotNull(retrieved, "Retrieved request should not be null");
        assertEquals(key, retrieved.get(RequestStore.KEY));
        assertEquals(transactionName, retrieved.get(RequestStore.TRANSACTION_NAME));
        assertEquals(scenarioName, retrieved.get(RequestStore.SCENARIO_NAME));
        assertEquals(String.valueOf(startTime), retrieved.get(RequestStore.START_TIME));

        // Verify directly in Redis
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> data = jedis.hgetAll(correlationId);
            assertEquals(key, data.get(RequestStore.KEY));
        }
    }
}
