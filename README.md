# Gatling Kafka Extension

A Gatling extension for load testing Kafka applications, with a focus on **Request-Reply (RPC)** patterns, **Quality of Service (QoS)** measurement, and **resilience testing**.

## Key Capabilities & Design Philosophy

This framework is designed not just to generate load, but to act as a precise instrument for measuring the **end-to-end quality of service** of your event-driven architecture.

### 1. End-to-End Latency Measurement
Unlike simple producer benchmarks, this extension measures the **full round-trip time** of a business transaction:
1.  **Send**: Gatling produces a request message.
2.  **Process**: Your system consumes, processes, and produces a response.
3.  **Receive**: Gatling consumes the response and matches it to the request.
4.  **Measure**: The reported latency is the true time the user waits, capturing producer delays, broker latency, consumer lag, and application processing time.

### 2. Quality of Service (QoS) & Error Protection
The extension protects against and measures various failure modes:
*   **Race Conditions**: By using a distributed `RequestStore` (Redis/Postgres), it handles scenarios where a response arrives before the request is fully acknowledged or when multiple Gatling nodes are active.
*   **Assurance of Delivery**:
    *   **At-Least-Once**: Configurable producer `acks=all` ensures requests are durably persisted before being tracked.
    *   **Idempotency**: Supports `enable.idempotence=true` to prevent duplicate requests from skewing test results.
*   **Data Integrity**: The `MessageCheck` API verifies that the response payload strictly matches expectations, catching subtle data corruption or logic errors under load.

### 3. Resilience & Outage Testing
A critical design goal is to measure the impact of system failures:
*   **Broker Outages**: When a broker fails, the extension tracks the exact impact on latency and throughput.
*   **Application Restarts**: If your application crashes and restarts, the extension's `RequestStore` ensures that pending requests are not lost. When the application recovers and sends delayed responses, Gatling correctly matches them and reports the high latency, providing a true picture of the outage's impact on the user experience.
*   **Chaos Engineering**: Perfect for validating system behavior during network partitions or component failures.

---

## Quick Start Example

Here is a complete example of a simulation that sends a Protobuf request and verifies the response.

```java
package pl.perfluencer.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.kafka.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaExampleSimulation extends Simulation {

    {
        // 1. Configure the Protocol
        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers("localhost:9092")
                .groupId("gatling-consumer-group")
                .numProducers(1) // Efficient batching with single producer
                .numConsumers(4) // Parallel processing of responses
                .producerProperties(Map.of(
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.LINGER_MS_CONFIG, "5",
                        ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"))
                .consumerProperties(Map.of(
                        "auto.offset.reset", "latest"));

        // 2. Define Message Checks (Validation)
        List<MessageCheck<?, ?>> checks = List.of(new MessageCheck<>(
                "Response Validation",
                String.class, SerializationType.STRING, // Request type
                String.class, SerializationType.STRING, // Response type
                (req, res) -> {
                    if (req.equals(res)) return Optional.empty(); // Pass
                    return Optional.of("Mismatch: " + req + " != " + res); // Fail
                }));

        // 3. Build the Scenario
        ScenarioBuilder scn = scenario("Kafka Request-Reply Demo")
                .exec(
                        KafkaDsl.kafkaRequestReply("request_topic", "response_topic",
                                session -> UUID.randomUUID().toString(), // Key
                                session -> "Hello Kafka " + UUID.randomUUID(), // Value (Object)
                                SerializationType.STRING,
                                checks,
                                10, TimeUnit.SECONDS)
                );

        // 4. Set up the Load Injection
        setUp(
                scn.injectOpen(constantUsersPerSec(10).during(30))
        ).protocols(kafkaProtocol);
    }
}
```

## Features

*   **Request-Reply Support**: Handle asynchronous request-reply flows where Gatling sends a request and waits for a correlated response on a different topic.
*   **Robust State Management**: Use external stores (PostgreSQL, Redis) to track in-flight requests, enabling tests to survive Gatling node restarts or long-running async processes.
*   **Flexible Serialization**: Support for String, ByteArray, Protobuf, Avro, and **custom Object payloads**.
*   **Message Validation**: Powerful `MessageCheck` API to validate response payloads against the original request.
*   **Fire-and-Forget**: High-performance mode for sending messages without waiting for broker acknowledgement.

## Core Components

*   **`KafkaProducerActor`**: Handles sending messages to Kafka. It uses the standard Kafka Producer API and supports generic `Object` payloads.
*   **`KafkaConsumerActor`**: Consumes messages from the response topic. It preserves metadata (headers, timestamp) and passes records to the processor.
*   **`RequestStore`**: Persists in-flight request data (correlation ID, payload, timestamp). Implementations include `InMemoryRequestStore` (default), `RedisRequestStore`, and `PostgresRequestStore`.
*   **`MessageCheck`**: A functional interface for validating the received response against the stored request.

## Basic Usage: Sending Messages

The `kafka` action allows you to send messages to a topic.

### Simple Send (Wait for Ack)
By default, the action waits for the Kafka broker to acknowledge the message (`acks=all` recommended).

```java
import static pl.perfluencer.kafka.javaapi.KafkaDsl.*;

// ... inside your scenario
.exec(
    kafka("request_topic", "my-key", "my-value")
)
```

### Fire and Forget
For maximum throughput where you don't need delivery guarantees or response tracking, use "Fire and Forget".

**Note:** In this mode, requests are **NOT** persisted to the Request Store (Redis/Postgres). This means you cannot track response times or verify replies for these messages.

```java
.exec(
    kafka("Fire Event", "events_topic", "key", "value", 
          false, // waitForAck = false
          30, TimeUnit.SECONDS)
)
```

### Raw Consumer (Consume Only)
Sometimes you need to consume messages from a topic without sending a request first (e.g., verifying a side-effect of another action).

You can use the `consume` action to read a message from a topic and save it to the session for validation.

```java
.exec(
    KafkaDsl.consume("Consume Event", "my_topic")
        .saveAs("myMessage") // Save the message value to session
)
.exec(session -> {
    String message = session.getString("myMessage");
    if (!"expected_value".equals(message)) {
        throw new RuntimeException("Unexpected message: " + message);
    }
    return session;
})
```

### Custom Headers

You can send custom headers with your Kafka messages. This is useful for passing metadata, tracing information, or routing details.

```java
.exec(
    kafka("Send with Headers", "my_topic", "my_key", "my_value")
        .headers(Map.of(
            "X-Correlation-ID", session -> UUID.randomUUID().toString(),
            "X-Source", session -> "Gatling-Load-Test",
            "X-Timestamp", session -> String.valueOf(System.currentTimeMillis())
        ))
)
```

## Using Feeders for Data-Driven Testing

Gatling feeders allow you to drive your Kafka tests with external data sources (CSV files, databases, or programmatically generated data). This is essential for realistic load testing with varied, production-like payloads.

### CSV Feeder Example

Create a CSV file at `src/test/resources/payment_data.csv`:

```csv
accountId,amount,currency,customerName
ACC-001,1500.00,USD,John Smith
ACC-002,2500.50,EUR,Jane Doe
ACC-003,750.25,GBP,Bob Wilson
```

Use it in your simulation:

```java
import static io.gatling.javaapi.core.CoreDsl.*;

public class PaymentSimulation extends Simulation {
    {
        // Load CSV feeder - circular() wraps around when exhausted
        FeederBuilder<String> csvFeeder = csv("payment_data.csv").circular();
        
        ScenarioBuilder scn = scenario("Payment Processing")
            .feed(csvFeeder)  // Inject data into each user's session
            .exec(
                KafkaDsl.kafkaRequestReply(
                    "payment-requests",
                    "payment-responses",
                    session -> session.getString("accountId"),  // Key from CSV
                    session -> String.format(
                        "{\"accountId\":\"%s\",\"amount\":%s,\"currency\":\"%s\"}",
                        session.getString("accountId"),
                        session.getString("amount"),
                        session.getString("currency")
                    ),
                    SerializationType.STRING,
                    paymentChecks,
                    10, TimeUnit.SECONDS
                )
            );
        
        setUp(scn.injectOpen(constantUsersPerSec(10).during(60)))
            .protocols(kafkaProtocol);
    }
}
```

### Random Feeder Example

Generate dynamic test data on-the-fly:

```java
FeederBuilder<Object> randomFeeder = Stream.of(
    Map.of(
        "randomAmount", () -> String.format("%.2f", Math.random() * 10000),
        "randomAccountId", () -> "ACC-" + UUID.randomUUID().toString().substring(0, 8),
        "timestamp", () -> String.valueOf(System.currentTimeMillis())
    )
).toFeeder();

ScenarioBuilder scn = scenario("Dynamic Payments")
    .feed(randomFeeder)
    .exec(
        KafkaDsl.kafkaRequestReply(
            "payments",
            "payment-responses",
            session -> session.getString("randomAccountId"),
            session -> String.format(
                "{\"accountId\":\"%s\",\"amount\":%s,\"ts\":%s}",
                session.getString("randomAccountId"),
                session.getString("randomAmount"),
                session.getString("timestamp")
            ),
            SerializationType.STRING,
            checks,
            10, TimeUnit.SECONDS
        )
    );
```

### Feeder Strategies

| Strategy | Usage | Best For |
|----------|-------|----------|
| `.queue()` | Each record used once | Unique transactions |
| `.circular()` | Wraps to start | Continuous load tests |
| `.random()` | Random selection | Varied data patterns |
| `.shuffle()` | Random order, once each | Unique + randomized |



This is the core feature of the extension. It allows you to test systems that consume a message, process it, and send a reply to another topic.

**Flow:**
1.  Gatling generates a unique `correlationId`.
2.  Gatling sends the request message (with `correlationId` in headers or payload) to the `request_topic`.
3.  Gatling stores the request details (key, payload, timestamp) in a **Request Store**.
4.  Your Application consumes the request, processes it, and sends a response to the `response_topic`.
5.  **Crucial:** The response MUST contain the same `correlationId` (usually in the header, configurable via `correlationHeaderName`).
6.  The Gatling Kafka Consumer reads the response.
7.  It looks up the original request in the **Request Store** using the `correlationId`.
8.  If found, it runs your **Message Checks** to validate the response against the request and records the transaction time in the Gatling report.

### Object Payloads & Serialization

The extension supports generic `Object` payloads. You can pass any object as the message value, provided you configure the appropriate serializer in the `KafkaProtocol`.

**Example with Custom Object:**

```java
// In Protocol Configuration
.producerProperties(Map.of(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyCustomSerializer.class.getName()
))

// In Scenario
.exec(
    KafkaDsl.kafkaRequestReply("req_topic", "res_topic",
        session -> "key",
        session -> new MyCustomObject("data"), // Pass Object directly
        SerializationType.BYTE_ARRAY, // Or custom enum
        checks,
        10, TimeUnit.SECONDS)
)
```

### Setting up a Request Store

You must configure a backing store for in-flight requests.

#### Option A: PostgreSQL (Recommended for Reliability)
Requires a `requests` table. See `src/main/resources/sql/create_requests_table.sql` (if available) or the schema below:

```sql
CREATE TABLE requests (
    correlation_id UUID PRIMARY KEY,
    request_key TEXT,
    request_value_bytes BYTEA,
    serialization_type VARCHAR(50),
    transaction_name VARCHAR(255),
    scenario_name VARCHAR(255),
    start_time TIMESTAMP,
    timeout_time TIMESTAMP,
    expired BOOLEAN DEFAULT FALSE
);

-- Recommended indexes for performance
CREATE INDEX idx_requests_timeout ON requests(timeout_time) WHERE NOT expired;
CREATE INDEX idx_requests_scenario ON requests(scenario_name);
```

**HikariCP Connection Pool Configuration:**
```java
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://localhost:5432/gatling");
config.setUsername("gatling");
config.setPassword("password");

// Connection pool sizing
config.setMaximumPoolSize(30);        // See sizing guide below
config.setMinimumIdle(5);             // Keep warm connections
config.setConnectionTimeout(10000);   // 10s max wait
config.setIdleTimeout(600000);        // 10min idle timeout
config.setMaxLifetime(1800000);       // 30min max lifetime
config.setLeakDetectionThreshold(60000); // Warn if connection held > 60s

// Performance optimizations
config.addDataSourceProperty("cachePrepStmts", "true");
config.addDataSourceProperty("prepStmtCacheSize", "250");
config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

HikariDataSource dataSource = new HikariDataSource(config);
```

**PostgreSQL Connection Pool Sizing:**

| Test Scale | Concurrent Writes | Recommended Pool Size | Postgres `max_connections` |
|------------|-------------------|----------------------|----------------------------|
| Small | < 50/sec | 10-20 | 100 |
| Medium | 50-200/sec | 20-40 | 200 |
| Large | 200-500/sec | 40-80 | 300 |
| Very Large | 500+/sec | Consider sharding | 500+ |

**Sizing Formula:**
```
Pool Size = (Concurrent Requests) × (Query Duration in sec) + 5 (overhead)
Example: 100 concurrent × 0.05s query time + 5 = 10 connections
```

> **Important**: PostgreSQL `max_connections` should be at least 2× the total pool size across all Gatling instances.

#### Option B: Redis (High Performance)
Requires a Redis instance. Stores payloads as Base64 strings.

**Connection Pool Configuration:**
```java
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

JedisPoolConfig poolConfig = new JedisPoolConfig();
poolConfig.setMaxTotal(50);        // Max connections (see sizing guide below)
poolConfig.setMaxIdle(20);         // Max idle connections
poolConfig.setMinIdle(5);          // Min idle connections for fast startup
poolConfig.setTestOnBorrow(true);  // Validate connections
poolConfig.setBlockWhenExhausted(true);
poolConfig.setMaxWaitMillis(3000); // Wait max 3s for connection

JedisPool pool = new JedisPool(poolConfig, "localhost", 6379);
```

**Redis Connection Pool Sizing Recommendations:**

| Test Scale | Users/sec | Recommended `maxTotal` | Notes |
|------------|-----------|------------------------|-------|
| Small (Dev) | < 10 | 10-20 | Default settings OK |
| Medium | 10-100 | 30-50 | Monitor connection waits |
| Large | 100-500 | 50-100 | Tune `minIdle` to 20-30 |
| Very Large | 500+ | 100-200 | Consider Redis clustering |

**Sizing Formula:**
```
maxTotal = (Peak Users/sec) × (Avg Request Duration in sec) × 1.5 (safety margin)
Example: 100 users/sec × 0.2s duration × 1.5 = 30 connections
```

**Performance Tips:**
- **Monitor**: Track "pool exhausted" errors in logs
- **Redis Memory**: Set `maxmemory` and use `allkeys-lru` eviction policy
- **Network**: Ensure low latency to Redis (< 1ms ideal)
- **Persistence**: Disable RDB/AOF for pure cache use cases

#### Option C: In-Memory Store (Development/Debugging Only)
Stores requests in a local `ConcurrentHashMap`. **Warning:** Data is lost on restart.

## Configuration & Performance Tuning

Proper configuration is critical for high-performance Kafka load testing.

### Producer Configuration

*   **`numProducers(int)`**: 
    *   **Default**: 1
    *   **Impact**: Kafka producers are thread-safe and high-throughput. A single producer can often saturate a network link.
    *   **Best Practice**: Start with **1**. Increasing this splits the load into smaller batches across multiple producers, which can *reduce* throughput and compression ratio. Only increase if you observe thread contention on the producer.
*   **`linger.ms`**:
    *   **Description**: Time to wait for more records to arrive before sending a batch.
    *   **Impact**: Higher values (e.g., 5-10ms) improve batching and throughput at the cost of slight latency.
    *   **Best Practice**: Set to `5` or `10` for high throughput. Set to `0` for lowest latency.
*   **`batch.size`**:
    *   **Description**: Maximum size of a batch in bytes.
    *   **Best Practice**: Increase to `65536` (64KB) or `131072` (128KB) for high throughput.
*   **`compression.type`**:
    *   **Best Practice**: Use `lz4` or `snappy` for a good balance of CPU usage and compression. `zstd` offers better compression but higher CPU cost.
*   **`acks`**:
    *   **`all`**: Highest durability (slowest).
    *   **`1`**: Leader acknowledgement (faster).
    *   **`0`**: Fire and forget (fastest, no guarantees).

### Idempotent Producers

To ensure that messages are delivered exactly once per partition (preventing duplicates due to retries), enable idempotence. This is often a prerequisite for transactional producers but can be used independently.

```java
KafkaProtocolBuilder protocol = kafka()
    .producerProperties(Map.of(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
        ProducerConfig.ACKS_CONFIG, "all", // Required for idempotence
        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString()
    ));
```

### Transactional Producers (Exactly-Once Semantics)

To enable transactional support (Exactly-Once Semantics), configure a `transactionalId`.

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .transactionalId("my-transactional-id") // Enables transactions
    .numProducers(4); // Each producer gets a unique ID: my-transactional-id-0, my-transactional-id-1, etc.
```

**Key Behaviors:**
*   **Atomic Writes**: Messages are written to the topic atomically. Consumers with `isolation.level=read_committed` will only see committed messages.
*   **Blocking Commit**: The producer will block to commit the transaction after each message (or batch). This ensures data integrity but may impact throughput.
*   **Unique IDs**: When `numProducers > 1`, the extension automatically appends a suffix (`-0`, `-1`, etc.) to the `transactionalId` to ensure each producer instance has a unique ID, preventing "Fenced Producer" errors.

### Consumer Configuration

*   **`numConsumers(int)`**:
    *   **Default**: 1
    *   **Impact**: Number of threads polling the response topic.
    *   **Best Practice**: Set this equal to the number of **partitions** of your response topic. Having more consumers than partitions is wasteful (idle threads).
*   **`fetch.min.bytes`**:
    *   **Best Practice**: Increase (e.g., `1024`) to reduce the number of fetch requests if you can tolerate slight latency.

### Consumer Isolation Level

When consuming from topics that contain transactional messages, you must decide whether to see all messages (including aborted transactions) or only committed ones.

*   **`read_uncommitted`** (Default): Consumers see all messages, including those from aborted transactions.
*   **`read_committed`**: Consumers only see messages from committed transactions. This is essential for Exactly-Once Semantics.

```java
KafkaProtocolBuilder protocol = kafka()
    .consumerProperties(Map.of(
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
    ));
```

### Example Optimized Configuration

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .numProducers(1)
    .numConsumers(4) // Matches response topic partitions
    .producerProperties(Map.of(
        ProducerConfig.ACKS_CONFIG, "1", // Faster than "all"
        ProducerConfig.LINGER_MS_CONFIG, "10", // Wait for batching
        ProducerConfig.BATCH_SIZE_CONFIG, "131072", // 128KB batches
        ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"
    ));
```

## Effective Assertions & Extraction

Validating that your system returns the *correct* data is just as important as measuring how fast it is. This section provides examples of how to verify response content.

### 1. Basic String Assertions
If your messages are simple strings (e.g., JSON or XML as text), you can use basic string manipulation.

```java
List<MessageCheck<?, ?>> checks = List.of(new MessageCheck<>(
    "Content Check",
    String.class, SerializationType.STRING,
    String.class, SerializationType.STRING,
    (req, res) -> {
        // Example 1: Check for a specific substring
        if (!res.contains("\"status\":\"success\"")) {
            return Optional.of("Response missing success status");
        }
        
        // Example 2: Check response length
        if (res.length() < 10) {
            return Optional.of("Response too short");
        }

        return Optional.empty(); // Success
    }
));
```

### 2. JSON Extraction & Validation
For JSON responses, it's best to parse the string to avoid fragile text matching. You can use libraries like Jackson or Gson (add them to your dependencies).

```java
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

// ...

ObjectMapper mapper = new ObjectMapper();

List<MessageCheck<?, ?>> checks = List.of(new MessageCheck<>(
    "JSON Logic Check",
    String.class, SerializationType.STRING,
    String.class, SerializationType.STRING,
    (req, res) -> {
        try {
            JsonNode root = mapper.readTree(res);
            
            // Check a specific field value
            int userId = root.path("user").path("id").asInt();
            if (userId <= 0) {
                return Optional.of("Invalid User ID: " + userId);
            }
            
            // Check array size
            if (root.path("items").size() < 1) {
                return Optional.of("Items array is empty");
            }
            
            return Optional.empty();
        } catch (Exception e) {
            return Optional.of("Failed to parse JSON: " + e.getMessage());
        }
    }
));
```

### 3. Extracting Values for Correlation
Sometimes you need to extract a value from the response to use in a subsequent request (though standard Request-Reply handles the correlation ID automatically).

If you need to extract a value from a JSON response body to correlate (instead of using headers), use the `JsonPathExtractor`:

```java
// Configure the protocol to look for the correlation ID in the JSON body
KafkaProtocolBuilder protocol = kafka()
    .correlationExtractor(
        // Extract the value at path $.meta.correlationId
        new JsonPathExtractor("$.meta.correlationId") 
    )
    // ... other config
    ;
```

### Configurable Correlation Header

By default, the extension uses a header named `correlationId` to track requests. If your application uses a different header name (e.g., `traceId`, `X-Request-ID`), you can configure it in the protocol.

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .correlationHeaderName("X-Request-ID") // Use custom header for correlation
    // ... other config

### Timestamp Extraction

By default, the extension uses the system clock to measure the end time of a transaction. However, you can configure it to use the timestamp from the Kafka message header instead. This is useful when you want to measure the time until the message was produced to the response topic, rather than when it was consumed by Gatling.

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .useTimestampHeader(true) // Use Kafka message timestamp as end time
    // ... other config
```
```

## Schema Registry Support (Avro & Protobuf)

The extension supports Avro and Protobuf serialization, integrated with Confluent Schema Registry.

### Dependencies

Add the following dependencies to your `pom.xml`:

```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.6.0</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-protobuf-serializer</artifactId>
    <version>7.6.0</version>
</dependency>
```

Ensure you have the Confluent repository configured:

```xml
<repositories>
    <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>
```

### Avro Example

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .producerProperties(Map.of(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
        "schema.registry.url", "http://localhost:8081"
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
        "schema.registry.url", "http://localhost:8081",
        "specific.avro.reader", "true" // If using specific Avro classes
    ));

// In your scenario
.exec(
    kafkaRequestReply("avro-req", "avro-res",
        session -> "key",
        session -> new User("Alice", 30), // Specific Avro Record
        SerializationType.AVRO, // Or BYTE_ARRAY if serializer handles it
        checks,
        10, TimeUnit.SECONDS)
)
```

### Protobuf Example

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .producerProperties(Map.of(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName(),
        "schema.registry.url", "http://localhost:8081"
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName(),
        "schema.registry.url", "http://localhost:8081",
        "specific.protobuf.value.type", Person.class.getName() // Required for specific type
    ));

// In your scenario
.exec(
    kafkaRequestReply("proto-req", "proto-res",
        session -> "key",
        session -> Person.newBuilder().setName("Bob").build(),
        SerializationType.PROTOBUF,
        checks,
        10, TimeUnit.SECONDS)
)
```

## Handling In-Flight Requests at Test End

When your load injection profile finishes, there may still be requests "in-flight" (sent but not yet replied to). If the simulation stops immediately, these requests will be lost or not recorded, potentially skewing your results.

To ensure all requests have a chance to complete (or timeout), you should add a "cooldown" period at the end of your injection profile using `nothingFor()`.

**Recommendation:** Set the cooldown duration to be at least as long as your request timeout.

```java
setUp(
    scn.injectOpen(
        rampUsersPerSec(10).to(100).during(60),
        constantUsersPerSec(100).during(300),
        
        // Cooldown: Wait for in-flight requests to complete or timeout
        nothingFor(Duration.ofSeconds(10)) 
    )
).protocols(kafkaProtocol);
```

## Integration Testing

This project includes comprehensive integration tests using [Testcontainers](https://testcontainers.com/).

### Running Integration Tests Locally

1. **Install Testcontainers Desktop** (Recommended) or configure a remote Docker daemon.
2. **Enable Tests**: Remove `@Ignore` annotations from test classes in `src/test/java/io/github/kbdering/kafka/integration/`.
3. **Run**: `mvn test`

**Troubleshooting:**
If you encounter `Could not find a valid Docker environment`, ensure your Docker daemon is running and accessible. You may need to set the `DOCKER_HOST` environment variable or configure `src/test/resources/testcontainers.properties`.

### Available Integration Tests

- **KafkaIntegrationTest**: End-to-end producer-consumer flow.
- **RedisIntegrationTest**: `RedisRequestStore` operations.
- **PostgresIntegrationTest**: `PostgresRequestStore` operations.
- **MockKafkaRequestReplyIntegrationTest**: Uses `MockProducer` and `MockConsumer` for fast, dependency-free testing of the actor logic.

---

## Docker Compose Setup

The easiest way to get started is using the provided Docker Compose environment, which includes Kafka (with TLS), Redis, PostgreSQL, and Kafka UI.

### Prerequisites
- Docker Desktop or Docker Engine + Docker Compose
- OpenSSL and Java keytool (for TLS certificates)

### Quick Start

```bash
# 1. Generate TLS certificates (first time only)
chmod +x scripts/generate-tls-certs.sh
./scripts/generate-tls-certs.sh

# 2. Start all services
docker-compose up -d

# 3. Verify services are healthy
docker-compose ps

# 4. Create test topics
docker exec gatling-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic request_topic \
  --partitions 4 \
  --replication-factor 1

docker exec gatling-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic response_topic \
  --partitions 4 \
  --replication-factor 1

# 5. Access Kafka UI (optional)
open http://localhost:8080
```

### Available Services

| Service | Port(s) | Description |
|---------|---------|-------------|
| **Kafka** | 9092 (PLAINTEXT), 9093 (TLS) | Message broker |
| **Zookeeper** | 2181 | Kafka coordination |
| **Redis** | 6379 | Request store (high performance) |
| **PostgreSQL** | 5432 | Request store (durable) |
| **Kafka UI** | 8080 | Web interface for Kafka |

### Using TLS/SSL with Kafka

The Docker Compose setup supports both plaintext (port 9092) and TLS (port 9093) connections.

#### Configure Gatling for TLS

```java
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import java.util.Map;

KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9093") // Use TLS port
    .producerProperties(Map.of(
        // TLS Configuration
        ProducerConfig.SECURITY_PROTOCOL_CONFIG, "SSL",
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "./tls-certs/kafka.client.truststore.jks",
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafkatest123",
        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "", // Disable for localhost
        
        // Performance settings
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"
    ))
    .consumerProperties(Map.of(
        // TLS Configuration
        "security.protocol", "SSL",
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "./tls-certs/kafka.client.truststore.jks",
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafkatest123",
        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""
    ));
```

> **Security Note**: The generated certificates use a test password (`kafkatest123`). For production, use a secrets management system (e.g., HashiCorp Vault, AWS Secrets Manager).

### Cleanup

```bash
# Stop services
docker-compose down

# Remove all data (Redis cache, Postgres DB)
docker-compose down -v
```

---


## Monitoring & Metrics

The Gatling Kafka extension uses standard Apache Kafka clients, which automatically expose internal metrics via **JMX (Java Management Extensions)**. You can monitor these metrics using tools like JConsole, VisualVM, or by attaching a Prometheus JMX Exporter.

### Key Metrics to Monitor

#### Producer Metrics (`kafka.producer:type=producer-metrics,client-id=*`)
*   **`record-send-rate`**: The average number of records sent per second.
*   **`record-error-rate`**: The average number of record sends that resulted in errors per second.
*   **`compression-rate`**: The average compression rate of record batches.
*   **`buffer-available-bytes`**: The total amount of buffer memory that is available (not currently used for buffering records). If this drops to 0, the producer will block.
*   **`request-latency-avg`**: The average request latency in ms.

#### Consumer Metrics (`kafka.consumer:type=consumer-metrics,client-id=*`)
*   **`records-consumed-rate`**: The average number of records consumed per second.
*   **`records-lag-max`**: The maximum lag in terms of number of records for any partition in this window. **Critical for verifying if consumers can keep up.**
*   **`fetch-latency-avg`**: The average time taken for a fetch request.

#### Consumer Fetch Metrics (`kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*,topic=*,partition=*`)
*   **`records-lag`**: The latest lag of the consumer for a specific partition.

### Viewing Metrics
1.  **JConsole / VisualVM**: Connect to the Gatling JVM process and navigate to the MBeans tab.
2.  **Prometheus JMX Exporter**: Configure the agent to scrape the MBeans listed above and visualize them in Grafana.

### 3. Asserting on JMX Metrics (Metric Injection)

You can inject internal Kafka client metrics (like consumer lag) directly into the Gatling simulation loop. This allows you to fail the test if the system is not healthy, even if response times are low.

**Enable Metric Injection:**

```java
KafkaProtocolBuilder protocol = kafka()
    // ... other config
    .metricInjectionInterval(Duration.ofSeconds(1)); // Inject metrics every second
```

**Assert on Metrics:**

Once enabled, metrics appear in the Gatling stats as pseudo-requests with the name "Kafka Metrics". You can use standard Gatling assertions on them.

```java
setUp(scn.injectOpen(atOnceUsers(1)))
    .protocols(protocol)
    .assertions(
        // Fail if max consumer lag exceeds 100 records
        details("Kafka Metrics").max("kafka-consumer-lag-max").lt(100),
        
        // Fail if we have any record errors
        details("Kafka Metrics").max("kafka-producer-record-error-rate").is(0.0)
    );
```

**Available Injected Metrics:**
*   `kafka-consumer-lag-max`
*   `kafka-producer-record-error-rate`
*   `kafka-producer-request-latency-avg`
*   `kafka-consumer-fetch-latency-avg`

### 4. Built-in Gatling Metrics

In addition to JMX metrics, the extension reports specific breakdown metrics to Gatling's StatsEngine for every transaction. This allows you to pinpoint latency sources.

| Metric Name Suffix | Description |
|---|---|
| `(none)` | The full **End-to-End Latency**. Includes: Send + Process + Store + Receive time. |
| `-send` | **Send Duration**: Time taken by the Kafka Producer to send the message and receive acknowledgement (if `waitForAck=true`). High values indicate network issues or broker congestion. |
| `-store` | **Store Duration**: Time taken to persist the request in the Request Store (Redis/Postgres). High values indicate database bottlenecks. |

**Example:**
If your request name is `PaymentRequest`:
- `PaymentRequest`: Total time (e.g., 150ms)
- `PaymentRequest-send`: Broker ack time (e.g., 20ms)
- `PaymentRequest-store`: Redis write time (e.g., 2ms)

You can use standard Gatling assertions on these breakdown metrics:
```java
// Assert that Redis write times are fast
details("PaymentRequest-store").responseTime().percentile99().lt(10)

// Assert that Broker ack times are reasonable
details("PaymentRequest-send").responseTime().percentile99().lt(50)
```

**Configuration:**
Detailed store latency metrics are **disabled by default** to minimize overhead. You can enable them if you need to debug request storage performance:

```java
KafkaProtocolBuilder protocol = kafka()
    // ...
    .measureStoreLatency(true); // Enable detailed store metrics
```

---

## 🔥 Resilience & Chaos Testing

One of the most powerful features of this extension is its ability to **measure the true impact of failures** on your event-driven architecture. Unlike simple throughput tests, this framework tracks end-to-end latency and data integrity during outages, giving you real insight into user experience during incidents.

### Why Traditional Load Tests Miss Failures

Most Kafka load tests only measure "happy path" performance:
- ✅ How many messages per second?
- ✅ What's the average latency?
- ❌ **What happens when a broker dies?**
- ❌ **How long does recovery take?**
- ❌ **Are any messages lost or corrupted?**

This framework answers those critical questions.

### Key Capabilities for Resilience Testing

1. **Persistent Request Tracking**: Using Redis/PostgreSQL, requests survive application crashes and restarts
2. **Timeout Detection**: Automatically detects and reports requests that never complete due to application failures
3. **End-to-End Measurement**: Captures the full impact of failures on user experience, including recovery time
4. **Data Validation**: Verifies responses are correct, not just "a response arrived"

---

## Chaos Testing Scenarios

### Scenario 1: Broker Outage During Load

**Test Objective**: Measure the impact of a broker failure on end-to-end latency and error rates.

**Setup**:
```java
// Configure with durable request store for failure resilience
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("broker1:9092,broker2:9092,broker3:9092")
    .requestStore(postgresStore) // Survives test disruptions
    .producerProperties(Map.of(
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.RETRIES_CONFIG, "10",
        ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "500"
    ))
    .consumerProperties(Map.of(
        "session.timeout.ms", "30000",
        "heartbeat.interval.ms", "3000"
    ));

ScenarioBuilder chaosScenario = scenario("Broker Outage Test")
    .exec(
        kafkaRequestReply("request-topic", "response-topic",
            session -> UUID.randomUUID().toString(),
            session -> generateTestPayload(),
            SerializationType.STRING,
            validationChecks,
            30, TimeUnit.SECONDS)
    );

setUp(
    chaosScenario.injectOpen(
        constantUsersPerSec(50).during(120) // 2 minutes of steady load
    )
).protocols(protocol);
```

**Test Procedure**:
1. Start the test (50 requests/sec baseline)
2. **At T+30s**: Kill a broker (e.g., `docker stop kafka-broker-2`)
3. **Observe**: Latency spikes as clients reconnect, some requests may time out
4. **At T+60s**: Restart the broker (`docker start kafka-broker-2`)
5. **Observe**: Recovery time, pending requests completing

**What to Measure**:
- **Latency Spike**: How high does P95/P99 latency go during the outage?
- **Error Rate**: What percentage of requests fail or timeout?
- **Recovery Time**: How long until latency returns to baseline?
- **Data Integrity**: Do all successful responses still pass validation?

**Expected Results** (healthy system):
- Brief latency spike (5-15 seconds) during metadata refresh
- No data corruption (all checks pass)
- Automatic recovery when broker returns

---

### Scenario 2: Consumer Group Rebalancing

**Test Objective**: Measure the impact of application deployment (rolling restart) on request-reply latency.

**Setup**:
```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .requestStore(redisStore) // Track requests during app restart
    .numConsumers(4) // Match your application's consumer count
    .pollTimeout(Duration.ofSeconds(1));

ScenarioBuilder rebalanceScenario = scenario("Consumer Rebalance Test")
    .forever()
    .exec(
        kafkaRequestReply("orders-in", "orders-out",
            session -> "order-" + session.userId(),
            session -> createOrder(),
            SerializationType.JSON,
            orderValidationChecks,
            45, TimeUnit.SECONDS) // Higher timeout for rebalance
        .pace(Duration.ofSeconds(1)) // 1 request per second per user
    );

setUp(
    rebalanceScenario.injectOpen(
        rampUsers(20).during(30),
        constantUsersPerSec(20).during(300), // 5 minutes steady state
        nothingFor(Duration.ofSeconds(60)) // Cooldown for pending requests
    )
).protocols(protocol);
```

**Test Procedure**:
1. Start test with 20 concurrent users
2. **At T+60s**: Restart one application instance (triggers rebalance)
3. **Observe**: Consumer group pauses, partition reassignment
4. **At T+120s**: Restart another application instance
5. **Observe**: Additional rebalance
6. **At T+180s**: Scale up application (add new instance)
7. **Observe**: Final rebalance

**What to Measure**:
- **Rebalance Duration**: How long does the consumer group pause?
- **Latency Impact**: Do requests queue up and complete later with higher latency?
- **Timeout Rate**: Do any requests timeout during the rebalance?
- **Message Loss**: Are all requests eventually processed?

**Gatling Assertions**:
```java
.assertions(
    global().responseTime().percentile3().lt(10000), // P99 < 10s even during rebalance
    global().successfulRequests().percent().gt(99.0) // 99% success rate
)
```

---

### Scenario 3: Partition Leader Election

**Test Objective**: Test behavior when a partition leader fails and a new leader must be elected.

**Setup**:
```java
// Target a specific partition for controlled chaos
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .requestStore(postgresStore)
    .producerProperties(Map.of(
        ProducerConfig.ACKS_CONFIG, "all", // Requires in-sync replicas
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1", // Strong ordering
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000"
    ));

// Use consistent partitioning to target specific partition
ScenarioBuilder partitionChaos = scenario("Leader Election Test")
    .exec(
        kafkaRequestReply("test-topic", "response-topic",
            session -> "fixed-key-partition-0", // Always partition 0
            session -> "test-message-" + System.nanoTime(),
            SerializationType.STRING,
            checks,
            20, TimeUnit.SECONDS)
    );
```

**Test Procedure**:
1. Start test targeting partition 0
2. **Identify leader**: `kafka-topics.sh --describe --topic test-topic`
3. **Kill leader broker**: Stop the broker hosting partition 0's leader
4. **Observe**: ISR shrinks, new leader elected from replicas
5. **Monitor**: Request latency during election (typically 5-30 seconds)

**Advanced Metrics Collection**:
```java
// Enable metric injection to track producer retry behavior
.metricInjectionInterval(Duration.ofMillis(500))

// Assert that retries happen but eventually succeed
.assertions(
    details("Kafka Metrics").max("kafka-producer-record-error-rate").lt(0.01),
    global().successfulRequests().percent().gt(95.0)
)
```

---

### Scenario 4: Application Crash and Resume

**Test Objective**: Verify that requests survive application crashes and are correctly measured when the app recovers.

**Key Insight**: This scenario showcases why `PostgresRequestStore` or `RedisRequestStore` is critical for resilience testing.

**Setup**:
```java
// Use PostgresRequestStore - survives crashes
PostgresRequestStore persistentStore = new PostgresRequestStore(
    dataSource,
    "requests"
);

KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .requestStore(persistentStore)
    .timeoutCheckInterval(Duration.ofSeconds(5));

ScenarioBuilder crashRecoveryScenario = scenario("Crash Recovery Test")
    .exec(
        kafkaRequestReply("crash-test-in", "crash-test-out",
            session -> UUID.randomUUID().toString(),
            session -> createComplexTransaction(),
            SerializationType.PROTOBUF,
            transactionChecks,
            120, TimeUnit.SECONDS) // Long timeout for crash recovery
    );
```

**Test Procedure**:
1. **T+0s**: Start test, requests flowing normally
2. **T+30s**: Force crash your application (`kill -9 <pid>`)
3. **Observe**: Requests stored in Postgres, waiting for responses
4. **T+60s**: Restart application
5. **Observe**: Application processes queued messages, sends responses
6. **Result**: Framework matches responses to requests, reports **true latency including downtime**

**What This Proves**:
- **No Silent Failures**: Every request is accounted for
- **True User Impact**: Latency includes the crash downtime (what users actually experience)
- **Data Durability**: Request metadata survives crashes

**Sample Results**:
```
Request sent at T+25s
Application crashes at T+30s (request in Postgres)
Application restarts at T+60s
Response received at T+65s
Reported latency: 40 seconds (TRUE user experience)
```

---

### Scenario 5: Network Partition (Split Brain)

**Test Objective**: Test behavior when Gatling can reach Kafka but the application cannot (or vice versa).

**Setup Using Toxiproxy**:
```bash
# Setup Toxiproxy to introduce network failures
docker run -d --name toxiproxy \
  -p 8474:8474 -p 9093:9093 \
  ghcr.io/shopify/toxiproxy

# Create proxy for Kafka broker
toxiproxy-cli create kafka-proxy \
  --listen 0.0.0.0:9093 \
  --upstream kafka:9092
```

**Gatling Configuration**:
```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("toxiproxy:9093") // Through proxy
    .requestStore(redisStore)
    .producerProperties(Map.of(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000",
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000"
    ));
```

**Test Procedure**:
1. Start test with normal traffic
2. **Inject latency**: `toxiproxy-cli toxic add -t latency -a latency=5000 kafka-proxy`
3. **Observe**: Slow responses, potential timeouts
4. **Inject packet loss**: `toxiproxy-cli toxic add -t slow_close kafka-proxy`
5. **Observe**: Connection failures, retry behavior

---

### Best Practices for Chaos Testing

1. **Use Persistent Request Stores**
   - InMemoryRequestStore: Development only
   - RedisRequestStore: Most chaos scenarios
   - PostgresRequestStore: Audit trail needed

2. **Set Realistic Timeouts**
   - Too short: False failures during recovery
   - Too long: Hides real problems
   - Recommendation: 2-3x normal P99 latency

3. **Add Cooldown Periods**
   ```java
   nothingFor(Duration.ofSeconds(60)) // Let pending requests complete
   ```

4. **Monitor Both Sides**
   - Gatling metrics (client view)
   - Kafka broker metrics (server view)
   - Application logs (processing view)

5. **Gradual Chaos Introduction**
   - Start with one failure type
   - Baseline performance first
   - Document expected vs. actual behavior

6. **Automate Chaos Injection**
   ```java
   exec(session -> {
       if (session.userId() % 100 == 0) {
           // Trigger chaos every 100 iterations
           Runtime.getRuntime().exec("scripts/kill-random-broker.sh");
       }
       return session;
   })
   ```

### Sample Test Report Metrics

After chaos testing, you should capture:

| Metric | Baseline | During Failure | Recovery Time |
|--------|----------|----------------|---------------|
| **Avg Latency** | 150ms | 8,500ms | 12 seconds |
| **P99 Latency** | 450ms | 35,000ms | 25 seconds |
| **Error Rate** | 0.01% | 12% | - |
| **Throughput** | 500/sec | 180/sec | 45 seconds |
| **Validation Failures** | 0 | 0 | - |

**Key Finding**: System degraded but **no data corruption** (validation passed for all completed requests).

---

## 🎓 Tips for Junior Developers

If you are new to Gatling or Kafka, this framework might look a bit intimidating. Here is a guide to help you get started without getting overwhelmed.

### 1. Start Simple: "It Just Works" Mode
Don't worry about Redis or Postgres yet. The framework uses an `InMemoryRequestStore` by default if you don't configure anything else.
*   **What it does**: Keeps track of your requests in the memory of the running Java process.
*   **Limitation**: If you stop the test, the data is gone. But for writing and debugging your first script, this is perfect.

### 2. Understanding `MessageCheck` (The "Scary" Part)
You will see code like `new MessageCheck<>(..., String.class, ...)` and it looks verbose. Think of it this way:
*   **Inputs**: You need to tell the framework "I sent a String" and "I expect a String back".
*   **Logic**: The lambda `(req, res) -> ...` is just a function where you compare the two.
*   **Tip**: Copy-paste the examples in this README! You rarely need to write this from scratch.

### 3. Fire-and-Forget vs. Request-Reply
*   **Use Request-Reply (Default)** when you need to verify that your application *actually processed* the message correctly. This is for **Quality**.
*   **Use Fire-and-Forget** (`waitForAck = false`) when you just want to see if your broker can handle 1 million messages per second. This is for **Quantity**.

### 4. IDE is Your Friend
The `kafkaRequestReply` method has many arguments. Use your IDE (IntelliJ/Eclipse) to help you:
*   **Cmd+P / Ctrl+P**: Shows you which parameter comes next.
*   **Auto-Complete**: Type `KafkaDsl.` and see what's available.

### 5. The "Bus vs. Taxi" Analogy (Batching & Latency)
Kafka is designed to move massive amounts of data, but you have to choose between **Efficiency (Throughput)** and **Speed (Latency)**.

*   **The Bus (High Throughput)**:
    *   **Scenario**: You want to move 10,000 people.
    *   **Strategy**: Wait for the bus to fill up (`linger.ms=10`).
    *   **Result**: High efficiency, fewer network requests.
    *   **Trade-off**: The first person on the bus has to wait for the last person before leaving.

*   **The Taxi (Low Latency)**:
    *   **Scenario**: You want to move 1 person *immediately*.
    *   **Strategy**: Leave right now (`linger.ms=0`).
    *   **Result**: Lowest possible latency for that person.
    *   **Trade-off**: Inefficient. You are sending a whole vehicle for just one person.

*   **The Fleet (Concurrency)**:
    *   **Scenario**: You have 10,000 people who ALL want "Taxi" speed.
    *   **Problem**: A single driver cannot drive 10,000 taxis at once.
    *   **Solution**: You need more drivers (`numProducers`).
    *   **Rule of Thumb**:
        *   **Standard**: `numProducers(1)` is enough for 90% of cases (Bus mode).
        *   **Low Latency**: If you force `linger.ms=0` (Taxi mode) AND have high volume, you MUST increase `numProducers` (e.g., 4-8) to handle the concurrency, otherwise requests will queue up waiting for the driver.

### 6. The Broker's Perspective (Why this matters)
It is important to understand that the Kafka Broker writes **Batches** to disk, not individual messages.
*   **1000 messages in 1 batch** = 1 Disk Write (Efficient).
*   **1000 messages in 1000 batches** = 1000 Disk Writes (Heavy Load).

**Your Goal**:
Unless you are specifically testing the Broker's limits (e.g., "Can it handle 50k IOPS?"), your goal is usually to measure the **End-to-End Latency** of your application.
*   Don't obsess over "Messages Per Second" if it means destroying your latency.
*   A realistic test often uses moderate batching (e.g., `linger.ms=5`) to simulate real-world traffic patterns where multiple users are active simultaneously.
