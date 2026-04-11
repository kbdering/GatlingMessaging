# Scala Integration

The Gatling Kafka Extension's Java API can be used directly from **Scala** simulations. This page covers the patterns needed to bridge the Java DSL into the Scala Gatling runtime.

## Prerequisites

Gatling 4.0+ no longer compiles Scala code natively. Bring in the `scala-maven-plugin` to compile your sources under `src/test/scala/`:

```xml
<plugin>
    <groupId>net.alchim31.maven</groupId>
    <artifactId>scala-maven-plugin</artifactId>
    <version>4.9.1</version>
    <executions>
        <execution>
            <goals><goal>testCompile</goal></goals>
        </execution>
    </executions>
    <configuration>
        <scalaVersion>2.13.14</scalaVersion>
    </configuration>
</plugin>
```

Alternatively, newer versions of the `gatling-maven-plugin` (v4.x+) can handle compilation if configured appropriately. For this project, we explicitly use the `scala-maven-plugin` for fine-grained control over test compilation.

</plugin>
```

Place your Scala simulations in `src/test/scala/`.

## Key Concepts

### `.asScala()` — Bridging Java Actions into Scala Scenarios

Every Java DSL action builder (`KafkaFireAndForget`, `KafkaRequestReply`, `KafkaConsumeActionBuilder`) exposes an `.asScala()` method that converts it into a Gatling core `ActionBuilder`, which can then be used inside Scala `.exec()` blocks:

```scala
.exec(
  kafka("Send Message")
    .send()
    .topic("my-topic")
    .key("my-key")
    .value("my-value")
    .asScala()  // converts to ScalaActionBuilder
)
```

### `.build()` — Converting the Protocol

The `KafkaProtocolBuilder` must be converted to a Gatling core `Protocol` using `.build()`:

```scala
setUp(
  scn.inject(atOnceUsers(1))
).protocols(kafkaProtocol.build())  // .build() required
```

## Session Access

There are **two ways** to access Gatling session variables from Scala:

=== "Java Session API"

    Use the standard `.key()` / `.value()` methods. The lambda receives a **Java** `io.gatling.javaapi.core.Session`:

    ```scala
    .exec { session =>
      session.set("myKey", UUID.randomUUID().toString)
    }
    .exec(
      kafka("Send")
        .send()
        .topic("topic")
        .key(session => session.getString("myKey"))  // Java Session
        .value(session => session.get("myPayload"))
        .asScala()
    )
    ```

=== "Scala Session API"

    Use `.keyScala()` / `.valueScala()` for idiomatic Scala access with `session("key").as[T]`:

    ```scala
    .exec { session =>
      session.set("myKey", UUID.randomUUID().toString)
    }
    .exec(
      kafka("Send")
        .send()
        .topic("topic")
        .keyScala(session => session("myKey").as[String])    // Scala Session
        .valueScala(session => session("myPayload").as[GenericRecord])
        .asScala()
    )
    ```

Both approaches work equivalently. The `keyScala`/`valueScala` variants are available on `KafkaFireAndForget`, `KafkaRequestReply`, and their header counterparts.

## Fire-and-Forget Example

```scala
import io.gatling.core.Predef._
import pl.perfluencer.kafka.javaapi.KafkaDsl._
import scala.concurrent.duration._
import java.util.UUID

class KafkaAvroScalaSimulation extends Simulation {

  val kafkaProtocol = kafka()
    .bootstrapServers("localhost:9092")
    .numProducers(1)
    .numConsumers(1)
    .producerProperties(java.util.Map.of(
      "key.serializer", classOf[StringSerializer].getName,
      "value.serializer", classOf[KafkaAvroSerializer].getName,
      "schema.registry.url", "http://localhost:8081"
    ))
    .consumerProperties(java.util.Map.of(
      "key.deserializer", classOf[StringDeserializer].getName,
      "value.deserializer", classOf[KafkaAvroDeserializer].getName,
      "schema.registry.url", "http://localhost:8081",
      "auto.offset.reset", "latest"
    ))

  val schema = new Schema.Parser().parse("""...""")

  val scn = scenario("Avro Send")
    .exec { session =>
      val record = new GenericData.Record(schema)
      record.put("name", "Alice")
      record.put("age", 30)
      session.set("myAvro", record)
    }
    .exec(
      kafka("Send Avro")
        .send()
        .topic("avro-topic")
        .key(session => UUID.randomUUID().toString)
        .value(session => session.get("myAvro"))
        .asAvro()
        .asScala()
    )
    .exec(
      consume("Consume Avro", "avro-topic")
        .asAvro(classOf[GenericRecord])
        .check(MessageCheck.responseNotEmpty())
        .asScala()
    )
    .pause(5)

  setUp(
    scn.inject(nothingFor(5.seconds), atOnceUsers(1))
  ).protocols(kafkaProtocol.build()).maxDuration(30.seconds)
}
```

!!! tip "Consumer Timing"
    Use `nothingFor(5.seconds)` before `atOnceUsers()` to give consumer threads time to subscribe before messages are produced. Add `.pause(5)` after consume actions to let background consumer threads poll and process.

## Request-Reply Example

```scala
import io.gatling.core.Predef._
import pl.perfluencer.kafka.javaapi.KafkaDsl._
import pl.perfluencer.kafka.MessageCheck
import pl.perfluencer.common.checks.Checks
import scala.concurrent.duration._
import java.util.UUID
import java.util.concurrent.TimeUnit

class KafkaRequestReplyScalaSimulation extends Simulation {

  val kafkaProtocol = kafka()
    .bootstrapServers("localhost:9092")
    .numProducers(1)
    .numConsumers(1)
    .producerProperties(java.util.Map.of(
      "key.serializer", classOf[StringSerializer].getName,
      "value.serializer", classOf[StringSerializer].getName
    ))
    .consumerProperties(java.util.Map.of(
      "key.deserializer", classOf[StringDeserializer].getName,
      "value.deserializer", classOf[StringDeserializer].getName,
      "auto.offset.reset", "latest"
    ))

  val scn = scenario("Request-Reply Flow")
    .exec { session =>
      session
        .set("orderId", UUID.randomUUID().toString.substring(0, 8))
        .set("myKey", UUID.randomUUID().toString)
    }
    .exec(
      kafka("Order Request")
        .requestReply()
        .asString()
        .requestTopic("orders-request")
        .responseTopic("orders-response")
        .key(session => session.getString("myKey"))
        .value(session =>
          s"""{"orderId":"${session.getString("orderId")}","action":"CREATE"}"""
        )
        .check(MessageCheck.responseNotEmpty())
        .check(Checks.jsonPath("$.status").find().exists())
        .timeout(10, TimeUnit.SECONDS)
        .asScala()
    )
    .pause(5)

  setUp(
    scn.inject(nothingFor(5.seconds), atOnceUsers(1))
  ).protocols(kafkaProtocol.build()).maxDuration(30.seconds)
}
```

## Running Scala Simulations

```bash
mvn gatling:test -Dgatling.simulationClass=pl.perfluencer.kafka.simulations.KafkaAvroScalaSimulation
```
