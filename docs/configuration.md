# Configuration & Performance Tuning

Proper configuration is critical for high-performance Kafka load testing and simulating real-world traffic accurately. All Kafka configuration happens within the `KafkaProtocolBuilder`.

## Base Protocol Setup

```java
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

KafkaProtocolBuilder protocol = KafkaDsl.kafka()
    .bootstrapServers("localhost:9092,localhost:9093")
    .groupId("gatling-loadtest-group")
    .numProducers(1)
    .numConsumers(4);
```

### Passing Raw Engine Properties

You can inject standard Apache Kafka properties directly into the Gatling producers and consumers:

```java
protocol
    .producerProperties(Map.of(
        ProducerConfig.ACKS_CONFIG, "1", // Faster than "all"
        ProducerConfig.LINGER_MS_CONFIG, "10", // Wait for batching
        ProducerConfig.BATCH_SIZE_CONFIG, "131072", // 128KB batches
        ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024"
    ));
```

### Advanced Protocol Settings

The `KafkaProtocolBuilder` offers several tuning knobs tailored for Gatling internals:

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    
    // Avoid Reflection overhead by registering manual parsers (huge CPU win)
    .registerParser(OrderResponse.class, bytes -> OrderResponse.parseFrom(bytes))
    
    // Use synchronous commit instead of async for higher delivery guarantees
    .syncCommit(true) 
    
    // How often background threads poll Kafka. Default: 100ms
    .pollTimeout(Duration.ofMillis(50)) 
    
    // How frequently the RequestStore sweeps for timeouts. Default: 5s
    .timeoutCheckInterval(Duration.ofSeconds(2));
```

## Tuning Producer Performance

* **`numProducers(int)`**: 
    * **Default**: 1
    * The underlying Kafka Client handles batching incredibly efficiently within a single thread. Increasing this splits the load into smaller batches, which can **reduce** your overall throughput and compression ratio. Only increase if you observe thread starvation locally.
* **`linger.ms`**:
    * Higher values (e.g., 5-10ms) improve batching and throughput at the cost of slight latency. Set to `0` for absolute minimal latency.
* **`batch.size`**:
    * Increase to `65536` (64KB) or `131072` (128KB) for high throughput.
* **`acks`**:
    * `all`: Highest durability (slowest). Ensures data is fully replicated in Kafka before the producer completes.
    * `1`: Leader acknowledgement (faster).
    * `0`: Fire and forget (fastest, no guarantees).

## Idempotence and Exactly-Once (Transactions)

When doing serious stress testing, clients can resend requests if a broker election happens. Enabling Idempotence protects your metrics from recording duplicate sends. 

Furthermore, if your microservices require Exactly-Once Semantics, you can configure Gatling as a transactional producer. 

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .transactionalId("my-transactional-id") // Enables transactions
    .numProducers(4) // Each gets a unique suffix: my-transactional-id-0, -1, etc.
    .producerProperties(Map.of(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
        ProducerConfig.ACKS_CONFIG, "all", // Required for idempotence/transactions
        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString()
    ));
```

> **Note**: When consuming from topics that contain transactional messages, configure your consumers to `isolation.level=read_committed`.

## Consumer Tuning

The most important consumer property is **`numConsumers`**. 

* **Default**: 1
* **Impact**: Number of background Gatling threads polling the response topic.
* **Best Practice**: Set this equal to the number of **partitions** of your test topic. Having more consumers than partitions is a waste of resource (they will sit idle).
