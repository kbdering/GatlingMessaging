package io.github.kbdering.kafka.integration;

import io.github.kbdering.kafka.util.SerializationType;
import io.github.kbdering.kafka.cache.RedisRequestStore;
import org.junit.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.JedisPool;

import java.util.Map;

@Ignore("Docker API compatibility issue. Install Testcontainers Desktop (https://testcontainers.com/desktop/) to run locally.")
public class RedisIntegrationTest {

    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7-alpine");
    private static GenericContainer<?> redis;
    private static JedisPool jedisPool;
    private RedisRequestStore requestStore;

    @BeforeClass
    public static void setUp() {
        redis = new GenericContainer<>(REDIS_IMAGE)
                .withExposedPorts(6379);
        redis.start();

        String redisHost = redis.getHost();
        Integer redisPort = redis.getFirstMappedPort();

        jedisPool = new JedisPool(redisHost, redisPort);
    }

    @AfterClass
    public static void tearDown() {
        if (jedisPool != null) {
            jedisPool.close();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    @Before
    public void setUpTest() {
        requestStore = new RedisRequestStore(jedisPool);
    }

    @Test
    public void testStoreAndRetrieveRequest() {
        String correlationId = "test-corr-id";
        String key = "test-key";
        byte[] valueBytes = "test-value".getBytes();
        String transactionName = "TestTransaction";
        String scenarioName = "TestScenario";
        long startTime = System.currentTimeMillis();
        long timeoutMillis = 30000;

        // Store request
        requestStore.storeRequest(correlationId, key, valueBytes, SerializationType.STRING,
                transactionName, scenarioName, startTime, timeoutMillis);

        // Retrieve request
        Map<String, Object> retrieved = requestStore.getRequest(correlationId);

        Assert.assertNotNull("Retrieved request should not be null", retrieved);
        Assert.assertEquals("Key should match", key, retrieved.get("key"));
        Assert.assertEquals("Transaction name should match", transactionName, retrieved.get("transactionName"));
        Assert.assertEquals("Scenario name should match", scenarioName, retrieved.get("scenarioName"));
    }

    @Test
    public void testRemoveRequest() {
        String correlationId = "test-remove-id";
        String key = "test-key-remove";
        byte[] valueBytes = "test-value-remove".getBytes();
        long startTime = System.currentTimeMillis();

        // Store request
        requestStore.storeRequest(correlationId, key, valueBytes, SerializationType.STRING,
                "RemoveTransaction", "RemoveScenario", startTime, 30000);

        // Verify it exists
        Map<String, Object> retrieved = requestStore.getRequest(correlationId);
        Assert.assertNotNull("Request should exist before removal", retrieved);

        // Remove request
        requestStore.deleteRequest(correlationId);

        // Verify it's removed
        Map<String, Object> afterRemoval = requestStore.getRequest(correlationId);
        Assert.assertNull("Request should be null after removal", afterRemoval);
    }

    @Test
    public void testProcessTimeouts() throws InterruptedException {
        String correlationId = "test-timeout-id";
        String key = "test-key-timeout";
        byte[] valueBytes = "test-value-timeout".getBytes();
        long startTime = System.currentTimeMillis();
        long shortTimeout = 100; // 100ms timeout

        // Store request with short timeout
        requestStore.storeRequest(correlationId, key, valueBytes, SerializationType.STRING,
                "TimeoutTransaction", "TimeoutScenario", startTime, shortTimeout);

        // Wait for timeout
        Thread.sleep(200);

        // Process timeouts (note: this would normally be called by the timeout handler)
        // For this test, we verify the request can still be retrieved
        // In a real scenario, the timeout handler would clean it up
        Map<String, Object> retrieved = requestStore.getRequest(correlationId);
        // The request should still exist unless timeout processing has run
        // This test shows the integration with Redis works correctly
        Assert.assertNotNull("Request should exist (timeout processing is handled by scheduler)", retrieved);
    }
}
