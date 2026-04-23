package pl.perfluencer.kafka.simulations

import io.gatling.core.Predef._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import pl.perfluencer.kafka.javaapi.KafkaDsl._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import pl.perfluencer.common.checks.Checks

import scala.concurrent.duration._
import java.util.UUID

class KafkaAvroJavaSessionSimulation extends Simulation {
  pl.perfluencer.kafka.integration.TestConfig.init()


  private val BOOTSTRAP_SERVERS = System.getProperty("kafka.bootstrap.servers", "localhost:9092")
  private val SCHEMA_REGISTRY_URL = System.getProperty("schema.registry.url", "http://localhost:8081")
  private val AVRO_TOPIC = "lab-avro-test-topic"

  val kafkaProtocol = kafka()
    .bootstrapServers(BOOTSTRAP_SERVERS)
    .groupId("gatling-java-avro-" + UUID.randomUUID().toString.substring(0, 8))
    .numProducers(1)
    .numConsumers(1)
    .pollTimeout(java.time.Duration.ofMillis(100))
    .producerProperties(java.util.Map.of(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName,
      "schema.registry.url", SCHEMA_REGISTRY_URL
    ))
    .consumerProperties(java.util.Map.of(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName,
      "schema.registry.url", SCHEMA_REGISTRY_URL,
      "auto.offset.reset", "latest"
    ))

  val schemaString =
    """
      |{
      |  "type":"record",
      |  "name":"User",
      |  "namespace":"pl.perfluencer.kafka.avro",
      |  "fields":[
      |    {"name":"name","type":"string"},
      |    {"name":"age","type":"int"}
      |  ]
      |}
      |""".stripMargin
  val schema = new Schema.Parser().parse(schemaString)

  // Demonstrates session variable propagation using Scala Session API
  // via keyScala() / valueScala(). These methods accept scala.Function1
  // and give you the native Gatling Scala Session with session("key").as[T].
  val scn = scenario("Send Avro Order with Scala Session")
    .exec { scalaSession =>
      val user: GenericRecord = new GenericData.Record(schema)
      user.put("name", "Alice Bob")
      user.put("age", 42)

      val correlationId = UUID.randomUUID().toString

      scalaSession
        .set("myAvroPayload", user)
        .set("myCorrelationId", correlationId)
    }
    .exec(
      kafka("Send Avro Message")
        .send()
        .topic(AVRO_TOPIC)
        // Scala Session API: session("key").as[T]
        .keyScala(session => session("myCorrelationId").as[String])
        .valueScala(session => session("myAvroPayload").as[GenericRecord])
        .asAvro()
        .asScala()
    )
    .exec(
      consume("Consume Avro Message", AVRO_TOPIC)
        .asAvro(classOf[org.apache.avro.generic.GenericRecord])
        .check(Checks.field(classOf[org.apache.avro.generic.GenericRecord])
          .get(rec => rec.get("name").toString)
          .satisfies(name => name.equals("Alice Bob")))
        .asScala()
    )
    .pause(5)

  setUp(
    scn.inject(nothingFor(5.seconds), atOnceUsers(1))
  ).protocols(kafkaProtocol.build()).maxDuration(30.seconds)
}
