# Resilience & Chaos Engineering

Most load tests assume a "happy path": They generate 50,000 req/s, write down the 5ms latency, and call the application scalable. 

However, Distributed Event-driven systems fail. The true value of this Gatling Extension is exposing **what actually happens to your End-User metrics when underlying infrastructure nodes are disrupted.** This is achieved heavily via the durable `RequestStore` (Redis/PostgreSQL).

Because Requests are durably tracked outside of the Gatling instance's volatile RAM, you can safely model chaotic infrastructure incidents without Gatling throwing false-positive correlation errors. 

## Scenario 1: The Application Crash & Resume

Verify that requests survive microservice crashes, and report the **True Queue Downtime** as part of latency.

```java
import pl.perfluencer.cache.PostgresRequestStore;

// 1. Point Gatling at an Enterprise durable state table
PostgresRequestStore persistentStore = new PostgresRequestStore(jdbcDataSource, "requests");

KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .requestStore(persistentStore);

ScenarioBuilder crashRecoveryScenario = scenario("Crash Recovery Test")
    .exec(
        kafka("Heavy Transactions")
            .requestReply()
            .requestTopic("tx-in")
            .responseTopic("tx-out")
            // Apply a massive 5 minute timeout explicitly allowing the queue to backup
            .timeout(5, TimeUnit.MINUTES)
    );

setUp(
    // Ramp up heavy load for 10 straight minutes
    crashRecoveryScenario.injectOpen(constantUsersPerSec(200).during(Duration.ofMinutes(10)))
).protocols(protocol);
```

### The Chaos Routine:
1. **Minute 1**: Execute Gatling. Baseline latency is 50ms.
2. **Minute 3**: Kill the microservice pod (`kubectl delete pod...` or `kill -9`). 
3. Gatling is still hammering Kafka. Consumer lag spikes uncontrollably. Gatling seamlessly logs Requests into Postgres. 
4. **Minute 5**: The microservice reboots and reconnects to Kafka.
5. The microservice frantically drains the queued topic, completing the work, and emitting responses. Gatling intercepts the responses.
6. Gatling pulls the pending state from Postgres. It successfully matches Correlation IDs!
7. **The True Metric**: Gatling reports these specific transactions taking **120,000ms (2 minutes)**. This is correct—it took two minutes to process those users' events!

## Scenario 2: Consumer Rebalancing Stutters

Test exactly how long your consumer groups "stop the world" when adding or removing nodes (e.g. Rolling Kubernetes deployments).

```java
ScenarioBuilder rebalanceScenario = scenario("Consumer Rebalance Profiling")
    .forever()
    .exec(
        kafka("Steady State")
            .requestReply()
            .requestTopic("orders-in")
            // ...
            .timeout(1, TimeUnit.MINUTES)
    )
    .pace(Duration.ofSeconds(1)); // Drip exactly 1 per second per user.

setUp(
    // 50 users trickling data
    rebalanceScenario.injectOpen(atOnceUsers(50))
).maxDuration(Duration.ofMinutes(15));
```

### The Chaos Routine:
1. Fire up exactly 3 microservice replicas on Kubernetes. 
2. Start Gatling drip-feed load testing.
3. Every 60 seconds, scale your replica count up and downwards (3 -> 4 -> 2 -> 3).
4. Review Gatling reports and `records-lag-max` JMX metrics. If your application isn't using cooperative rebalancing, you will see nasty jagged 30-second spikes in your Gatling 99th percentile graphs as the partitions pause!

## Scenario 3: Kafka Broker Failure

Ensure producers don't blackhole data or lock up entirely if a single Kafka broker suffers a hardware failure.

Ensure `acks=all` so the producer refuses to complete the timing loop until In-Sync-Replicas guarantee data safety.

```java
KafkaProtocolBuilder protocol = kafka()
    .producerProperties(Map.of(
        ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.RETRIES_CONFIG, "10",
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"
    ));
```

Shut down one of your Kafka JVM processes mid-test. Latency will immediately jump as Producers get fenced and wait for the Controller to promote a remaining replica as the new Leader. 
