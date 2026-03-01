# Getting Started

This guide will help you install the Gatling Kafka Extension, configure a basic simulation, and run your first test.

## Prerequisites

- Java 11 or higher (Java 17+ recommended)
- Maven 3.6+
- A running Kafka cluster (or you can use Docker/Testcontainers)

## 1. Add Dependencies

Add the Gatling Kafka OSS extension to your `pom.xml` dependencies along with the core Gatling libraries.

```xml
<dependencies>
    <!-- Gatling Kafka Extension -->
    <dependency>
        <groupId>pl.perfluencer</groupId>
        <artifactId>gatling-kafka-oss</artifactId>
        <version>1.0.0-SNAPSHOT</version> <!-- Use the latest version -->
        <scope>test</scope>
    </dependency>

    <!-- Core Gatling Dependencies -->
    <dependency>
        <groupId>io.gatling.highcharts</groupId>
        <artifactId>gatling-charts-highcharts</artifactId>
        <version>3.10.3</version>
        <scope>test</scope>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>io.gatling</groupId>
            <artifactId>gatling-maven-plugin</artifactId>
            <version>4.7.0</version>
        </plugin>
    </plugins>
</build>
```

## 2. Your First Simulation

Here is a complete, minimal simulation demonstrating a Request-Reply flow using JSON payloads.

Create a file at `src/test/java/org/example/KafkaExampleSimulation.java`:

```java
package org.example;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;
import static pl.perfluencer.kafka.javaapi.Kafka.kafka;
import static pl.perfluencer.common.checks.Checks.*;

public class KafkaExampleSimulation extends Simulation {

    {
        // 1. Configure the Protocol
        KafkaProtocolBuilder protocol = KafkaDsl.kafka()
                .bootstrapServers("localhost:9092")
                // Use multiple producers/consumers for higher throughput
                .numProducers(4)
                .numConsumers(4);

        // 2. Build the Scenario
        ScenarioBuilder scn = scenario("Kafka Request-Reply Demo")
                .exec(
                    kafka("Order Request")
                        .requestReply()
                        .requestTopic("orders-request")
                        .responseTopic("orders-response")
                        // Generate a unique key for correlation matching
                        .key(session -> UUID.randomUUID().toString())
                        // The payload sent to Kafka
                        .value("{\"orderId\":\"ORD-123\", \"action\":\"CREATE\"}")
                        
                        // Validate that the async response contains expected data
                        .check(jsonPath("$.status").find().is("OK"))
                        .check(substring("SUCCESS").find().exists())
                        
                        // Wait maximum 10 seconds for the response
                        .timeout(10, TimeUnit.SECONDS)
                );

        // 3. Set up the Load Injection Profile
        setUp(
            scn.injectOpen(
                // Warmup with constant users, then ramp up
                constantUsersPerSec(5).during(Duration.ofSeconds(10)),
                rampUsersPerSec(5).to(50).during(Duration.ofSeconds(30))
            )
        ).protocols(protocol);
    }
}
```

## 3. Running the Test

Run the simulation using the Gatling Maven plugin:

```bash
mvn gatling:test -Dgatling.simulationClass=org.example.KafkaExampleSimulation
```

When the test finishes, Gatling will automatically generate an HTML report providing detailed statistics on your End-to-End latency, error rates, and throughput.

## Docker Quick Start (Local Kafka)

If you don't have a Kafka broker running locally, you can quickly spin one up using Docker Compose. Create a `docker-compose.yml` file:

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

Start the cluster:
```bash
docker-compose up -d
```
