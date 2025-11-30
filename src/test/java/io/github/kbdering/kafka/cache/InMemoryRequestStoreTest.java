package io.github.kbdering.kafka.cache;

import io.github.kbdering.kafka.util.SerializationType;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.*;

public class InMemoryRequestStoreTest {

    private InMemoryRequestStore store;

    @Before
    public void setUp() {
        store = new InMemoryRequestStore();
    }

    @Test
    public void testStoreAndRetrieveRequest() {
        String correlationId = "corr-123";
        String key = "key-1";
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        long startTime = System.currentTimeMillis();
        String transactionName = "test-transaction";
        String scenarioName = "test-scenario";

        store.storeRequest(correlationId, key, value, SerializationType.STRING, transactionName, scenarioName,
                startTime, 1000);

        Map<String, Object> retrieved = store.getRequest(correlationId);
        assertNotNull(retrieved);
        assertEquals(transactionName, retrieved.get(RequestStore.TRANSACTION_NAME));
        assertEquals(scenarioName, retrieved.get(RequestStore.SCENARIO_NAME));
        assertEquals(String.valueOf(startTime), retrieved.get(RequestStore.START_TIME));
        assertEquals(SerializationType.STRING, retrieved.get(RequestStore.SERIALIZATION_TYPE));

        store.deleteRequest(correlationId);
        assertNull(store.getRequest(correlationId));
    }

    @Test
    public void testRemoveNonExistentRequest() {
        store.deleteRequest("non-existent");
        assertNull(store.getRequest("non-existent"));
    }

    @Test
    public void testVariableTimeouts() throws Exception {
        // Use a store with a short check interval
        store = new InMemoryRequestStore(10);

        final java.util.concurrent.atomic.AtomicInteger timeoutCount = new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.List<String> timedOutIds = new java.util.concurrent.CopyOnWriteArrayList<>();

        store.startTimeoutMonitoring(new TimeoutHandler() {
            @Override
            public void onTimeout(String correlationId, Map<String, Object> requestData) {
                timeoutCount.incrementAndGet();
                timedOutIds.add(correlationId);
            }
        });

        long now = System.currentTimeMillis();
        // Store request 1 with 100ms timeout
        store.storeRequest("req-1", "key", "val".getBytes(), SerializationType.STRING, "t1", "s1", now, 100);
        // Store request 2 with 300ms timeout
        store.storeRequest("req-2", "key", "val".getBytes(), SerializationType.STRING, "t2", "s2", now, 300);

        // Wait 150ms - req-1 should timeout, req-2 should not
        Thread.sleep(150);
        assertEquals("Should have 1 timeout", 1, timeoutCount.get());
        assertTrue("req-1 should have timed out", timedOutIds.contains("req-1"));

        // Wait another 200ms (total 350ms) - req-2 should timeout
        Thread.sleep(200);
        assertEquals("Should have 2 timeouts", 2, timeoutCount.get());
        assertTrue("req-2 should have timed out", timedOutIds.contains("req-2"));

        store.close();
    }
}
