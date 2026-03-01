# Gatling Kafka Extension

Welcome to the official documentation for the **Gatling Kafka Extension**, an advanced, open-source load testing tool designed for event-driven architectures. 

Built on top of Gatling, this extension is uniquely focused on **Request-Reply (RPC) patterns**, precise **Quality of Service (QoS)** measurements, and evaluating system **resilience** under diverse failure scenarios.

---

## Why this extension?

Traditional Kafka benchmarks focus entirely on producer throughput—testing how fast you can shove messages onto a topic. However, most modern microservices are not simple fire-and-forget streams; they involve complex synchronous-like interactions over async topics.

This framework is built to test **End-to-End Latency**:
1. **Send**: Gatling produces a request message.
2. **Process**: Your application consumes the message, executes business logic, and produces a response to a different topic.
3. **Receive**: Gatling consumes the response and correlates it accurately back to the original request.
4. **Measure**: The reported latency represents the true time a user waits, encompassing producer delay, broker latency, consumer lag, and total application processing time.

## Key Capabilities

### 1. Robust State Management
For request-reply scenarios under massive load, race conditions are common. Sometimes a response arrives *before* the request is even fully acknowledged or tracked by the Gatling producer. 
This extension uses distributed `RequestStore` implementations (InMemory, Redis, Postgres) to reliably track in-flight requests, enabling distributed Gatling clusters to maintain consistent state.

### 2. Quality of Service (QoS) Guarantees
- **At-Least-Once Delivery**: Wait for Kafka `acks=all` before starting the clock.
- **Idempotency**: Prevent duplicate requests from skewing your test results.
- **Data Integrity**: Validate the exact payload returned by your application using a powerful `MessageCheck` API.

### 3. Resilience & Outage Testing
Measure what happens to your users when things go wrong:
- **Broker Outages**: What happens to latency when a broker node fails?
- **Application Restarts**: If your microservice crashes, the `RequestStore` ensures Gatling doesn't lose track. When the app recovers, Gatling measures the true queued latency.
- **Split Brains**: Test how well your application handles Kafka cluster network partitions.

### 4. Enterprise Grade Metrics
Beyond response times, inject and assert on internal JMX metrics like `records-lag-max` directly within your Gatling scenarios to proactively fail tests if consumers can't keep up.

---

## Next Steps

- Get up and running in minutes with the [Getting Started](getting-started.md) guide.
- Check out the [Core Architecture](architecture.md) to understand how the Request-Reply engine works.
- Learn how to validate complex data structures in [Assertions & Validation](checks-assertions.md).
