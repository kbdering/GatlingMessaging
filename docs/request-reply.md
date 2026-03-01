# Request-Reply & State

Gatling HTTP load tests map perfectly to HTTP connections (Client opens a socket, Request goes out, Response comes back, socket closes). Kafka is totally decoupled. The Gatling Producer fires an event and immediately moves on. Sometime later, a background Consumer Thread receives a message. 

The framework bridges this async gap using **Correlation Contexts** stored in a **Request Store**.

## Configuring the Flow

A `requestReply()` block defines the lifecycle from request injection to response extraction.

```java
import static pl.perfluencer.kafka.javaapi.KafkaDsl.*;

ScenarioBuilder scn = scenario("RPC Flow")
    .exec(
        kafka("Process Order")
            .requestReply()
            .requestTopic("orders_in")
            .responseTopic("orders_out")
            // A unique value tying the sent object and received object together
            .key(session -> UUID.randomUUID().toString()) 
            .value("{\"cmd\": \"start\"}")
            .check(...)
            
            // Passthrough Session variables to the response matcher
            .storeSession("userId", "scenarioType")
            
            // Do not store the large payload in memory (memory optimization)
            .skipPayloadStorage()
            
            // Critical: If the consumer receives no correlation match within 10s, fail
            .timeout(10, TimeUnit.SECONDS)
    );
```

### Memory Optimization: `skipPayloadStorage()`
If your payloads are very large (e.g., 500KB XML) and you do not need to verify the *Request* payload during the *Response* check phase, you can append `.skipPayloadStorage()` to the builder. This prevents the large payload from entering the Request Store, drastically reducing JVM heap pressure at high concurrency.

### Session Context: `storeSession()`
By default, the Gatling Session that sent the request is isolated from the Gatling Session that receives the reply asynchronously. If you need variables from the sending Session to exist when evaluating the Response checks (or in subsequent HTTP steps), declare them using `.storeSession("key1", "key2")`.

## Correlation Strategies

Gatling needs to extract the Correlation ID from the incoming Response to match it against pending operations in the Request Store.

### 1. Key Matching (Default)
By default, Gatling assumes the Kafka Record Key of the *Response* matches the Correlation ID generated during the *Request*. 

### 2. Header Matching
If your architecture passes a tracking ID deep through headers across services, you can match on that instead. 

```java
KafkaProtocolBuilder protocol = kafka()
    .correlationHeaderName("X-Trace-Id"); // Look for correlation ID in this header
```

When building the request:
```java
kafka("Transaction")
    .requestReply()
    .requestTopic("req")
    .responseTopic("res")
    // Generate the ID, and place it in the same header you expect back
    .headers(Map.of(
        "X-Trace-Id", session -> UUID.randomUUID().toString()
    ))
    .value("...")
```

### 3. Payload Extraction
Sometimes legacy or locked-down applications pack the Correlation ID directly into the data payload itself. You can provide an extractor to the protocol.

```java
import pl.perfluencer.kafka.extractors.JsonPathExtractor;

KafkaProtocolBuilder protocol = kafka()
    .correlationExtractor(
        new JsonPathExtractor("$.meta.traceId")
    );
```

## Dealing with Fast Responses (Race Conditions)

In exceptionally low latency environments, a highly tuned microservice might consume a request and emit the response *faster* than Gatling's Producer thread can persist the state into the `RequestStore` (especially if writing to a remote Postgres database).

When the Gatling Consumer thread sees this response, it looks in the Request Store, finds nothing, and normally drops the event as "Uncorrelated". Then the Producer finally writes the state, but the Consumer has already moved on, resulting in a false Timeout.

To protect against this, use **Asynchronous Match Retry**:

```java
KafkaProtocolBuilder protocol = kafka()
    .retryBackoff(Duration.ofMillis(50)) // Wait 50ms before retrying a check
    .maxRetries(3);                      // Retry 3 times before failing
```
If the Consumer does not find a match, instead of throwing immediately, it yields and waits.

## Fire And Forget

If you are simulating purely unidirectional event ingestion where no reply is expected, skip the state tracking entirely for maximum throughput:

```java
.exec(
    kafka("Fire Event")
        .send()  // Not requestReply()
        .topic("events_topic")
        .key("key")
        .value("value")
)
```
*Note: You cannot track response times or verify replies in `send()` mode.*
