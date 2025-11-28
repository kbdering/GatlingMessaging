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
package io.github.kbdering.kafka.simulations;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.github.kbdering.kafka.javaapi.KafkaDsl;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;
import io.github.kbdering.kafka.util.SerializationType;
import io.github.kbdering.kafka.MessageCheck;
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
import static io.github.kbdering.kafka.javaapi.KafkaDsl.*;

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
```

#### Option B: Redis (High Performance)
Requires a Redis instance. Stores payloads as Base64 strings.

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

### Consumer Configuration

*   **`numConsumers(int)`**:
    *   **Default**: 1
    *   **Impact**: Number of threads polling the response topic.
    *   **Best Practice**: Set this equal to the number of **partitions** of your response topic. Having more consumers than partitions is wasteful (idle threads).
*   **`fetch.min.bytes`**:
    *   **Best Practice**: Increase (e.g., `1024`) to reduce the number of fetch requests if you can tolerate slight latency.

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

### Available Integration Tests

- **KafkaIntegrationTest**: End-to-end producer-consumer flow.
- **RedisIntegrationTest**: `RedisRequestStore` operations.
- **PostgresIntegrationTest**: `PostgresRequestStore` operations.
- **MockKafkaRequestReplyIntegrationTest**: Uses `MockProducer` and `MockConsumer` for fast, dependency-free testing of the actor logic.
