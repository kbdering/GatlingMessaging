package pl.perfluencer.kafka.integration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import pl.perfluencer.kafka.cache.BatchProcessor;
import pl.perfluencer.kafka.cache.PostgresRequestStore;
import pl.perfluencer.kafka.cache.TimeoutHandler;
import pl.perfluencer.kafka.util.SerializationType;
import org.junit.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

// @Ignore("Docker API compatibility issue. Install Testcontainers Desktop (https://testcontainers.com/desktop/) to run locally.")
public class PostgresIntegrationTest {

    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:16-alpine");
    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private PostgresRequestStore requestStore;

    @BeforeClass
    public static void setUp() throws SQLException {
        postgres = new PostgreSQLContainer<>(POSTGRES_IMAGE)
                .withDatabaseName("testdb")
                .withUsername("testuser")
                .withPassword("testpass");
        postgres.start();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(5);

        dataSource = new HikariDataSource(config);

        // Initialize schema
        initializeSchema(dataSource);
    }

    @AfterClass
    public static void tearDown() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Before
    public void setUpTest() {
        requestStore = new PostgresRequestStore(dataSource);
    }

    @After
    public void cleanUpTest() throws SQLException {
        // Clean up test data after each test
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute("TRUNCATE TABLE requests");
        }
    }

    private static void initializeSchema(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            // Table name must match PostgresRequestStore (requests)
            String createTableSQL = "CREATE TABLE IF NOT EXISTS requests (" +
                    "correlation_id UUID PRIMARY KEY," +
                    "request_key VARCHAR(255)," +
                    "request_value_bytes BYTEA," +
                    "serialization_type VARCHAR(50)," +
                    "transaction_name VARCHAR(255)," +
                    "scenario_name VARCHAR(255)," +
                    "start_time TIMESTAMP," +
                    "timeout_time TIMESTAMP," +
                    "expired BOOLEAN DEFAULT FALSE" +
                    ")";
            conn.createStatement().execute(createTableSQL);

            String createIndexSQL = "CREATE INDEX IF NOT EXISTS idx_timeout_time ON requests(timeout_time)";
            conn.createStatement().execute(createIndexSQL);
        }
    }

    @Test
    public void testStoreAndRetrieveRequest() {
        String correlationId = UUID.randomUUID().toString();
        String key = "test-key";
        byte[] valueBytes = "test-value".getBytes();
        String transactionName = "PostgresTransaction";
        String scenarioName = "PostgresScenario";
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
    public void testBatchedRecords() {
        String correlationId1 = UUID.randomUUID().toString();
        String correlationId2 = UUID.randomUUID().toString();
        long startTime = System.currentTimeMillis();

        requestStore.storeRequest(correlationId1, "key1", "value1".getBytes(), SerializationType.STRING, "txn", "scn",
                startTime, 0);
        requestStore.storeRequest(correlationId2, "key2", "value2".getBytes(), SerializationType.STRING, "txn", "scn",
                startTime, 0);

        Map<String, Object> records = new HashMap<>();
        records.put(correlationId1, "response1");
        records.put(correlationId2, "response2");

        TestBatchProcessor processor = new TestBatchProcessor();
        requestStore.processBatchedRecords(records, processor);

        assertEquals(2, processor.matched.size());
        assertTrue(processor.matched.containsKey(correlationId1));
        assertTrue(processor.matched.containsKey(correlationId2));

        // Verify they are removed
        assertNull(requestStore.getRequest(correlationId1));
        assertNull(requestStore.getRequest(correlationId2));
    }

    @Test
    public void testRemoveRequest() {
        String correlationId = UUID.randomUUID().toString();
        requestStore.storeRequest(correlationId, "key", "val".getBytes(), SerializationType.STRING, "txn", "scn",
                System.currentTimeMillis(), 0);

        assertNotNull(requestStore.getRequest(correlationId));

        requestStore.deleteRequest(correlationId);
        assertNull(requestStore.getRequest(correlationId));
    }

    @Test
    public void testProcessTimeouts() throws InterruptedException {
        String correlationId = UUID.randomUUID().toString();
        // Set a short timeout
        requestStore.storeRequest(correlationId, "key", "val".getBytes(), SerializationType.STRING, "txn", "scn",
                System.currentTimeMillis(), 100);

        TestTimeoutHandler handler = new TestTimeoutHandler();
        requestStore.startTimeoutMonitoring(handler);

        // Wait for timeout processing
        Thread.sleep(1000);
        // Trigger manually to be sure, although startTimeoutMonitoring starts a
        // scheduler
        requestStore.processTimeouts();

        assertTrue(handler.timedOutIds.contains(correlationId));
        assertNull(requestStore.getRequest(correlationId));
    }

    // Helper class for testBatchedRecords
    private static class TestBatchProcessor implements BatchProcessor {
        public final Map<String, Map<String, Object>> matched = new ConcurrentHashMap<>();

        @Override
        public void onMatch(String correlationId, Map<String, Object> requestData, Object responseValue) {
            matched.put(correlationId, requestData);
        }

        @Override
        public void onUnmatched(String correlationId, Object responseValue) {
            // No-op for test
        }
    }

    // Helper class for testProcessTimeouts
    private static class TestTimeoutHandler implements TimeoutHandler {
        public final Set<String> timedOutIds = ConcurrentHashMap.newKeySet();

        @Override
        public void onTimeout(String correlationId, Map<String, Object> requestData) {
            timedOutIds.add(correlationId);
        }
    }
}
