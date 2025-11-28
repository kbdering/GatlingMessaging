# Gatling Kafka Extension

A Gatling extension for load testing Kafka applications, with a focus on **Request-Reply (RPC)** patterns and asynchronous messaging.

## Features

*   **Request-Reply Support**: Handle asynchronous request-reply flows where Gatling sends a request and waits for a correlated response on a different topic.
*   **Robust State Management**: Use external stores (PostgreSQL, Redis) to track in-flight requests, enabling tests to survive Gatling node restarts or long-running async processes.
*   **Flexible Serialization**: Support for String, Protobuf, and Avro serialization.
*   **Message Validation**: Powerful `MessageCheck` API to validate response payloads against the original request.
*   **Fire-and-Forget**: High-performance mode for sending messages without waiting for broker acknowledgement.

## Architecture

Unlike traditional synchronous protocols (like HTTP), this extension is fully **asynchronous**:

1.  **Non-Blocking Actions**: When Gatling executes a `kafkaRequestReply` action, it sends the message, stores the request state, and **immediately proceeds** to the next action in the scenario. It does *not* block the virtual user waiting for a response.
2.  **Consumer Notification**: A background `KafkaConsumerActor` listens for responses. When a response arrives, it looks up the corresponding request in the **Request Store**, calculates the response time, and **notifies the Gatling Stats Engine**.
3.  **Distributed State**: By using an external Request Store (Postgres/Redis), request state is decoupled from the Gatling node, allowing for robust testing of long-running asynchronous processes.

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.kbdering</groupId>
    <artifactId>gatling-kafka</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Basic Usage: Sending Messages

The `kafka` action allows you to send messages to a topic.

### Simple Send (Wait for Ack)
By default, the action waits for the Kafka broker to acknowledge the message (`acks=all` recommended).

```java
import static io.github.kbdering.kafka.javaapi.KafkaDsl.*;

// ... inside your scenario
.exec(
    kafka("request_topic", "my-key", "my-value")
)
```

### Custom Request Name
You can provide a custom name for the request, which will appear in the Gatling report.

```java
.exec(
    kafka("Send Login Event", "events_topic", "user-123", "login_payload")
)
```

### Fire and Forget
For maximum throughput where you don't need delivery guarantees or response tracking, use "Fire and Forget".

**Key Behavior:**
*   Sends the message and immediately proceeds.
*   **Does NOT** wait for broker acknowledgement (`acks=0` behavior from the client perspective, though the producer config still applies).
*   **Does NOT** interact with the Request Store (no overhead).

```java
.exec(
    kafka("Fire Event", "events_topic", "key", "value", 
          false, // waitForAck = false
          30, TimeUnit.SECONDS)
)
```

---

## Request-Reply Pattern

This is the core feature of the extension. It allows you to test systems that consume a message, process it, and send a reply to another topic.

**Flow:**
1.  Gatling generates a unique `correlationId`.
2.  Gatling sends the request message (with `correlationId` in headers or payload) to the `request_topic`.
3.  Gatling stores the request details (key, payload, timestamp) in a **Request Store**.
4.  Your Application consumes the request, processes it, and sends a response to the `response_topic`.
5.  **Crucial:** The response MUST contain the same `correlationId` (usually in the header).
6.  The Gatling Kafka Consumer reads the response.
7.  It looks up the original request in the **Request Store** using the `correlationId`.
8.  If found, it runs your **Message Checks** to validate the response against the request and records the transaction time in the Gatling report.

> **Note:** The `kafkaRequestReply` action is non-blocking. The virtual user continues immediately after sending. The "Response Time" recorded in the report is the time between sending the request and the consumer receiving the reply.

### 1. Setting up a Request Store

You must configure a backing store for in-flight requests.

#### Option A: PostgreSQL (Recommended for Reliability)
Requires a `requests` table.

**Schema:**
```sql
CREATE TABLE requests (
    correlation_id UUID PRIMARY KEY,
    request_key TEXT,
    request_value_bytes BYTEA,
    serialization_type VARCHAR(50),
    transaction_name VARCHAR(255),
    start_time TIMESTAMP,
    timeout_time TIMESTAMP,
    expired BOOLEAN DEFAULT FALSE
);
```

**Java Setup:**
```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
config.setUsername("user");
config.setPassword("pass");
DataSource dataSource = new HikariDataSource(config);

RequestStore postgresStore = new PostgresRequestStore(dataSource);

KafkaProtocolBuilder protocol = kafka()
    .requestStore(postgresStore)
    // ... other config
```

#### Option B: Redis (High Performance)
Requires a Redis instance.

**Java Setup:**
```java
JedisPool jedisPool = new JedisPool("localhost", 6379);
RequestStore redisStore = new RedisRequestStore(jedisPool);

KafkaProtocolBuilder protocol = kafka()
    .requestStore(redisStore)
    // ... other config
```

#### Option C: In-Memory Store (Development/Debugging Only)
Stores requests in a local `ConcurrentHashMap`.

**Use Cases:**
*   Local debugging and development.
*   Unit tests or small-scale integration tests.
*   Scenarios where Gatling node restarts are not a concern.

**Drawbacks:**
*   **Scalability:** Limited by the JVM heap size. Large tests with many in-flight requests may cause `OutOfMemoryError`.
*   **Reliability:** **Data Loss on Restart.** If the Gatling node crashes or is restarted, all in-flight request state is lost, and pending responses will not be matched. **Do not use for long-running tests or production-grade load testing.**

**Java Setup:**
```java
RequestStore memoryStore = new InMemoryRequestStore();

KafkaProtocolBuilder protocol = kafka()
    .requestStore(memoryStore)
    // ... other config
```

### 2. Serialization and Checks

The extension supports validating responses using `MessageCheck`. You can compare the received response with the stored request.

#### String Serialization
Simple text-based validation.

```java
List<MessageCheck<?, ?>> checks = new ArrayList<>();
checks.add(new MessageCheck<>(
    "String Equality Check",
    String.class, SerializationType.STRING,
    String.class, SerializationType.STRING,
    (req, res) -> {
        if (req.equals(res)) return Optional.empty(); // Pass
        return Optional.of("Mismatch: " + req + " != " + res); // Fail
    }
));

// Usage
kafkaRequestReply("req_topic", "res_topic", 
    session -> "key", 
    session -> "value".getBytes(StandardCharsets.UTF_8), 
    SerializationType.STRING, 
    checks, 
    10, TimeUnit.SECONDS
)
```

#### Protobuf Serialization
For binary protocols, use `SerializationType.PROTOBUF`.

**Prerequisites:**
1.  Generate your Protobuf Java classes (e.g., `MyRequest`, `MyResponse`).
2.  Ensure your application propagates the `correlationId`.

**Example:**
```java
List<MessageCheck<?, ?>> protoChecks = new ArrayList<>();
protoChecks.add(new MessageCheck<>(
    "Status Check",
    MyRequest.class, SerializationType.PROTOBUF,
    MyResponse.class, SerializationType.PROTOBUF,
    (req, res) -> {
        if (!res.getSuccess()) {
            return Optional.of("Operation failed");
        }
        if (!req.getId().equals(res.getEchoedId())) {
            return Optional.of("ID Mismatch");
        }
        return Optional.empty();
    }
));

// Usage
kafkaRequestReply("req_topic", "res_topic",
    session -> UUID.randomUUID().toString(), // Key
    session -> MyRequest.newBuilder()        // Value (as byte[])
                .setId("123")
                .setData("test")
                .build().toByteArray(),
    SerializationType.PROTOBUF,
    protoChecks,
    10, TimeUnit.SECONDS
)
```

#### Avro Serialization
Similar to Protobuf, but use `SerializationType.AVRO`. You are responsible for serializing your Avro objects to `byte[]` in the DSL and deserializing them in the check logic (or the extension handles deserialization if you provide the class and it has a standard decoder).

*Note: For complex Avro setups (Schema Registry), you may need to handle serialization manually in the lambda and use `byte[]` checks.*
234: 
235: ### 3. Correlation Strategies
236: 
237: By default, the extension correlates requests and responses using a `correlationId` header. However, you can configure different strategies.
238: 
239: #### Header Correlation (Default)
240: Gatling generates a UUID and sends it in the `correlationId` header. The response must contain the same header.
241: 
242: #### Value Correlation (JSON Path)
If your system embeds the correlation ID in the response message body (JSON), you can use the `JsonPathExtractor`.
244: 
245: ```java
246: import io.github.kbdering.kafka.CorrelationExtractor;
247: 
248: // ...
249: 
250: CorrelationExtractor extractor = KafkaDsl.jsonPath("$.meta.correlationId");
251: 
252: KafkaProtocolBuilder protocol = kafka()
253:     .correlationExtractor(extractor)
254:     // ...
255: ```
256: 
257: #### Key Correlation
258: If the Kafka message key acts as the correlation ID, the extension will automatically fall back to checking the key if no header or body extractor finds a match.

## Configuration Reference

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .groupId("gatling-group")
    .numProducers(1) // Shared producers per node
    .numConsumers(4) // Consumers for response topic
    .requestStore(store)
    .producerProperties(Map.of(
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.LINGER_MS_CONFIG, "5"
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"
    ));
```

## Performance Tuning

### Producers
*   **`numProducers(1)`**: Start with 1 producer per Gatling node. Kafka producers are thread-safe and generally high-performance.
*   **Batching & Load Profile**: Using a single producer maximizes batching efficiency (larger batches, better compression) because all virtual users send through the same accumulator.
    *   **Concern**: "Won't 1 producer be a bottleneck?" -> Usually, no. A single Kafka producer can handle millions of messages/sec.
    *   **When to increase**: If you see high CPU usage on the producer I/O thread or if lock contention occurs (rare).
    *   **Trade-off**: Increasing `numProducers` splits the load. For the same total throughput, this results in *smaller* batches per producer, which can actually *decrease* overall throughput and increase broker load (more requests). Only increase if the single producer is saturated.

### Consumers
*   **`numConsumers(4)`**: This controls the number of threads polling the response topic.
*   **Recommendation**: Set this to match the number of partitions of your response topic, or a multiple of it if you are doing heavy processing in the checks.
*   **Thread Pool**: These consumers run in a dedicated actor system. Ensure your Gatling node has enough cores to handle the context switching if you have many consumers.

## Testing Kafka Broker Resilience

This extension is designed to act as a **stable source of truth** when testing the resilience of your Kafka cluster or application.

### Goal
Verify that your system handles broker failures, leader elections, and network partitions without losing data, while Gatling accurately tracks every request.

### 1. Gatling as the Source of Truth
To ensure Gatling only records a request as "sent" when it is durably persisted in Kafka:

*   **`acks=all`**: The producer will wait for the full set of in-sync replicas to acknowledge the record.
*   **`enable.idempotence=true`**: Ensures that even if the producer retries due to a network error, the message is written exactly once to the log.

### 2. Surviving Outages with Persistent Store
If your system goes down for an extended period (e.g., a full cluster restart), Gatling needs to remember the pending requests.

*   **Use `PostgresRequestStore`**: In-memory stores will lose data if the Gatling node crashes or is restarted. A database ensures that even if Gatling is restarted, it can reload the pending requests and match them when the system recovers.

### 3. Configuration Example
Here is a configuration optimized for resilience testing, allowing for longer timeouts and ensuring maximum durability.

```java
KafkaProtocolBuilder protocol = kafka()
    .requestStore(new PostgresRequestStore(dataSource)) // 1. Persistent Store
    .producerProperties(Map.of(
        ProducerConfig.ACKS_CONFIG, "all",              // 2. Strong durability guarantees
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true", // 3. No duplicates
        ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE), // 4. Keep trying
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000" // 5. Allow 2 minutes for recovery
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed", // Only read committed data
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
    ));
```

### 4. Test Scenario
1.  **Start Load**: Run Gatling with the above configuration.
2.  **Inject Failure**: Kill a Kafka broker, disconnect the network, or restart the application consumers.
3.  **Observe**:
    *   **Producers**: May block or increase latency during leader election, but should eventually succeed (due to high retries).
    *   **Consumers**: Will stop receiving responses during the outage.
    *   **Recovery**: Once the system is back, Gatling consumers will catch up. The `PostgresRequestStore` will match the late responses to the original requests.
    *   **Report**: You will see a spike in response times corresponding to the outage duration, but **zero data loss** (no unexpected KO requests).

## Integration Testing

This project includes comprehensive integration tests using [Testcontainers](https://testcontainers.com/) to verify functionality with real Kafka, Redis, and PostgreSQL instances.

### Running Integration Tests Locally

Due to Docker API compatibility requirements, integration tests are disabled by default. To run them locally:

1. **Install Testcontainers Desktop** (Recommended)
   - Download from: https://testcontainers.com/desktop/
   - Provides a compatible Docker environment
   - Free for local development

2. **Alternative: Remote Docker Daemon**
   ```bash
   export DOCKER_HOST=tcp://your-compatible-docker-host:2375
   ```

3. **Enable Tests**
   - Remove `@Ignore` annotations from test classes in `src/test/java/io/github/kbdering/kafka/integration/`
   - Or run specific tests: `mvn test -Dtest=RedisIntegrationTest`

### Available Integration Tests

- **KafkaIntegrationTest**: End-to-end producer-consumer flow with real Kafka broker
- **RedisIntegrationTest**: `RedisRequestStore` operations with real Redis instance  
- **PostgresIntegrationTest**: `PostgresRequestStore` operations with real PostgreSQL database

### CI/CD

Integration tests run automatically in GitHub Actions where Docker compatibility is managed by the pipeline environment.
