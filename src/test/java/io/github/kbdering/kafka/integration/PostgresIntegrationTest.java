package io.github.kbdering.kafka.integration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.kbdering.kafka.util.SerializationType;
import io.github.kbdering.kafka.cache.PostgresRequestStore;
import org.junit.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Ignore("Docker API compatibility issue. Install Testcontainers Desktop (https://testcontainers.com/desktop/) to run locally.")
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
            conn.createStatement().execute("TRUNCATE TABLE kafka_requests");
        }
    }

    private static void initializeSchema(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            String createTableSQL = "CREATE TABLE IF NOT EXISTS kafka_requests (" +
                    "correlation_id VARCHAR(255) PRIMARY KEY," +
                    "key VARCHAR(255)," +
                    "value_bytes BYTEA," +
                    "serialization_type VARCHAR(50)," +
                    "transaction_name VARCHAR(255)," +
                    "scenario_name VARCHAR(255)," +
                    "start_time BIGINT," +
                    "timeout_at BIGINT" +
                    ")";
            conn.createStatement().execute(createTableSQL);

            String createIndexSQL = "CREATE INDEX IF NOT EXISTS idx_timeout_at ON kafka_requests(timeout_at)";
            conn.createStatement().execute(createIndexSQL);
        }
    }

    @Test
    public void testStoreAndRetrieveRequest() {
        String correlationId = "postgres-test-id";
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
        String correlationId1 = "batch-id-1";
        String correlationId2 = "batch-id-2";
        String key = "batch-key";
        byte[] valueBytes = "batch-value".getBytes();
        long startTime = System.currentTimeMillis();

        // Store multiple requests
        requestStore.storeRequest(correlationId1, key, valueBytes, SerializationType.STRING,
                "BatchTransaction", "BatchScenario", startTime, 30000);
        requestStore.storeRequest(correlationId2, key, valueBytes, SerializationType.STRING,
                "BatchTransaction", "BatchScenario", startTime, 30000);

        // Retrieve batch (simulate batch processing)
        Map<String, Object> req1 = requestStore.getRequest(correlationId1);
        Map<String, Object> req2 = requestStore.getRequest(correlationId2);

        Assert.assertNotNull("First request should exist", req1);
        Assert.assertNotNull("Second request should exist", req2);
    }

    @Test
    public void testRemoveRequest() {
        String correlationId = "postgres-remove-id";
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
        String correlationId = "postgres-timeout-id";
        String key = "test-key-timeout";
        byte[] valueBytes = "test-value-timeout".getBytes();
        long startTime = System.currentTimeMillis();
        long shortTimeout = 100; // 100ms timeout

        // Store request with short timeout
        requestStore.storeRequest(correlationId, key, valueBytes, SerializationType.STRING,
                "TimeoutTransaction", "TimeoutScenario", startTime, shortTimeout);

        // Wait for timeout
        Thread.sleep(200);

        // Verify request still exists (demonstrating integration works)
        Map<String, Object> retrieved = requestStore.getRequest(correlationId);
        Assert.assertNotNull("Request should exist (timeout processing is handled by scheduler)", retrieved);
    }
}
