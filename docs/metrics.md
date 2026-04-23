# Metrics & Monitoring

Gatling produces comprehensive HTML reports out-of-the-box. The standard Request-Reply latency timings reflect the End-to-End time (Application execution + Producer overhead + Broker network + Consumer transit).

However, diagnosing exactly *why* numbers are degrading requires looking deeper.

## Breakdown Metrics (Internal Latency) 

The extension reports specific breakdown metrics to Gatling's StatsEngine for every transaction, allowing you to pinpoint the source of a slowdown.

| Metric | Sub-Segment Details |
|---|---|
| `RequestName` | Pure End-to-End Latency. |
| `RequestName-send` | **Send Duration**: Time taken by Gatling's Kafka Producer to send the message and receive acknowledgement (`acks=all`). High values indicate Kafka broker congestion or network failure. |
| `RequestName-store` | **Store Duration**: Time Gatling took to durably write into the Postgres/Redis `RequestStore`. High values indicate your test rig's caching tier is overwhelmed. |

You can assert against these breakdown metrics specifically:
```java
// Fail if the Redis caching tier is lagging behind
details("Checkout-store").responseTime().percentile99().lt(10)

// Fail if the Kafka Broker is struggling to ack
details("Checkout-send").responseTime().percentile99().lt(50)
```
*(Note: Fine-grain store latency metrics are disabled by default. Enable via `.measureStoreLatency(true)` on the ProtocolBuilder).*

### Report Layout (ACK vs E2E)

The two metrics produced per `requestReply()` action appear as separate flat rows in the Gatling HTML report's Global Statistics table:

| Row | What it measures | Grouped? |
|---|---|---|
| `RequestName send` | Broker ACK latency — time from `producer.send()` to receiving the broker acknowledgment | **Always ungrouped** (global level) |
| `RequestName` | Full End-to-End latency — time from sending the request to the consumer thread receiving the correlated reply | Inherits the user's `.group()` context |

The `send` row is intentionally kept **ungrouped** regardless of what `.group()` block the simulation uses. This prevents double-counting: if the `send` also contributed to a group's cumulated time, the group total would be `ACK ms + E2E ms`, but since the E2E period already includes the ACK window the resulting aggregate would be misleading.

#### Using Gatling's native `.group()` for folder organisation

If you want the E2E metrics to appear nested under a named folder in the report, wrap the action using the standard Gatling group DSL:

```java
// Java DSL
ScenarioBuilder scn = scenario("Order Flow")
    .group("Checkout").on(
        exec(
            kafka("Place Order")
                .requestReply()
                .asString()
                .requestTopic("orders")
                .responseTopic("orders-reply")
                .value(session -> "{\"id\":\"" + session.getString("orderId") + "\"}")
        )
    );
```

```scala
// Scala DSL
val scn = scenario("Order Flow")
  .group("Checkout").on(
    exec(
      kafka("Place Order")
        .requestReply()
        .asString()
        .requestTopic("orders")
        .responseTopic("orders-reply")
        .value(session => s"""{"id":"${session.getString("orderId")}"}""")
        .asScala()
    )
  )
```

The resulting report will look like:

```
Global Statistics
──────────────────────────────────────────────────
Place Order send    1000   0   48ms   120ms   ← always at global level
──────────────────────────────────────────────────
▼ Checkout                                    ← your .group() folder
    Place Order     1000   2  312ms   980ms   ← E2E metric nested here
──────────────────────────────────────────────────
```


---

## Metric Injection (JMX Internal Extraction)

The Gatling Kafka extension uses the official Apache Kafka Java clients internally. These clients automatically expose an incredibly rich array of diagnostics via **JMX (Java Management Extensions)**.

You can tell the Gatling extension to routinely actively scrape these internal JMX metrics and inject them directly into your HTML report as pseudo-requests!

### Enabling Metric Injection

```java
KafkaProtocolBuilder protocol = kafka()
    // Extract consumer/producer telemetry every exactly 1,000 milliseconds
    .metricInjectionInterval(Duration.ofSeconds(1)); 
```

Once enabled, metrics appear in Gatling reports underneath a dummy request group named `"Kafka Metrics"`.

### JMX Gatling Assertions

The most dangerous scenario in messaging is silent consumer lag accumulation. Your API might be receiving requests fast (P99 < 10ms HTTP), but the background workers are burying themselves in depth, leading to ultimate catastrophe. 

Instead of waiting for timeouts, use Injected Metrics to instantly fail Gatling builds if internal system health deteriorates!

```java
setUp(scn.injectOpen(...))
    .assertions(
        // Fail the CI build if the Kafka consumer falls behind by more than 100 messages anywhere
        details("Kafka Metrics").max("kafka-consumer-lag-max").lt(100),
        
        // Fail immediately if there was a sudden burst of internal IO Producer errors
        details("Kafka Metrics").max("kafka-producer-record-error-rate").is(0.0)
    );
```

### Available Injected JMX Paths
- `kafka-consumer-lag-max`: Critical! Indicates consumers cannot keep up.
- `kafka-producer-record-error-rate`: Rate of background failure inside the client thread.
- `kafka-producer-request-latency-avg`: Client-to-Broker average HTTP-like latency.
- `kafka-consumer-fetch-latency-avg`: Poll cycle waiting duration.

## Monitoring External Dashboards

If you want independent out-of-band monitoring rather than polling within Gatling processes, connect standard JVM JMX Exporters. 
Start your Gatling process with Prometheus JMX Java agents injected, scraping the `kafka.producer` and `kafka.consumer` MBean sub-trees for visualization directly in Grafana.
