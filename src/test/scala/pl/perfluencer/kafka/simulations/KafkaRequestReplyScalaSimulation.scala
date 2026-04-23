package pl.perfluencer.kafka.simulations

import io.gatling.core.Predef._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import pl.perfluencer.kafka.javaapi.KafkaDsl._
import pl.perfluencer.kafka.MessageCheck
import pl.perfluencer.common.checks.Checks

import scala.concurrent.duration._
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Demonstrates the Request-Reply pattern from Scala.
 *
 * This simulation sends a JSON request to one topic, then waits for a
 * correlated response on the same topic (echo pattern). It uses the Java
 * Session API for key/value and shows how to add checks and timeouts.
 */
class KafkaRequestReplyScalaSimulation extends Simulation {
  pl.perfluencer.kafka.integration.TestConfig.init()


  private val BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092")
  private val REQUEST_TOPIC = "lab-rr-scala-topic"

  val kafkaProtocol = kafka()
    .bootstrapServers(BOOTSTRAP_SERVERS)
    .groupId("gatling-rr-scala-" + UUID.randomUUID().toString.substring(0, 8))
    .numProducers(1)
    .numConsumers(1)
    .pollTimeout(java.time.Duration.ofMillis(100))
    .producerProperties(java.util.Map.of(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
    ))
    .consumerProperties(java.util.Map.of(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName,
      "auto.offset.reset", "latest"
    ))

  val scn = scenario("Scala Request-Reply")
    .exec { session =>
      val orderId = UUID.randomUUID().toString.substring(0, 8)
      val correlationId = UUID.randomUUID().toString
      session
        .set("orderId", orderId)
        .set("myCorrelationId", correlationId)
    }
    .exec(
      kafka("Request-Reply Order")
        .requestReply()
        .asString()
        .requestTopic(REQUEST_TOPIC)
        .responseTopic(REQUEST_TOPIC)
        // Java Session API — session.getString()
        .key(session => session.getString("myCorrelationId"))
        .value(session => s"""{"orderId":"${session.getString("orderId")}","status":"NEW"}""")
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
