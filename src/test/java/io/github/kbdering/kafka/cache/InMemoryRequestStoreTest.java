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
}
