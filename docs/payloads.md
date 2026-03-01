# Payloads & Serialization

The extension supports generic `Object` payloads, meaning you can pass complex objects directly as values without serializing them manually in your scenario logic. Gatling leverages the rich ecosystem of existing standard Kafka Serializers.

## Standard Strings and JSON

For simpler formats, you stringify the payload inside Gatling and rely on the Kafka `StringSerializer`.

```java
 KafkaProtocolBuilder protocol = KafkaDsl.kafka()
    .producerProperties(Map.of(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
    ));
```

```java
.exec(
    kafka("Send JSON")
        .requestReply()
        .requestTopic("req")
        .responseTopic("res")
        .key(session -> "key")
        .value(session -> "{\"user\": \"" + session.userId() + "\"}")
        // Use the built-in type shortcut
        .asString()
)
```

## Schema Registry Support (Avro & Protobuf)

The extension supports Avro and Protobuf serializations natively integrated with the Confluent Schema Registry.

Ensure you have the proper dependencies in `pom.xml`:
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.6.0</version>
</dependency>
```

### Avro Example (GenericRecord)

When interacting with dynamically generated Avro payloads, you often use `GenericRecord` instead of compiled POJOs.

```java
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

KafkaProtocolBuilder protocol = kafka()
    .producerProperties(Map.of(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
        "schema.registry.url", "http://localhost:8081"
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName(),
        "schema.registry.url", "http://localhost:8081"
    ));

// Pre-parse the schema once outside the execution loops
final Schema USER_SCHEMA = new Schema.Parser().parse("...");

ScenarioBuilder scn = scenario("Avro Tests")
    .exec(
        kafka("Avro Request")
            .requestReply()
            .requestTopic("avro-req")
            .responseTopic("avro-res")
            .key(session -> "key")
            // Pass the un-serialized Object! 
            .value(session -> {
                GenericRecord user = new GenericData.Record(USER_SCHEMA);
                user.put("name", "User-" + session.userId());
                return user;
            })
            // Tell Gatling to expect an Avro deserialization back
            .asAvro(GenericRecord.class, GenericRecord.class)
            .check("Valid Avro", (GenericRecord req, GenericRecord res) -> {
                // Complex validation between Request Record and Response Record
                return Optional.empty();
            })
    )
```

### Protobuf Example

```java
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

KafkaProtocolBuilder protocol = kafka()
    .producerProperties(Map.of(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class.getName(),
        "schema.registry.url", "http://localhost:8081"
    ))
    .consumerProperties(Map.of(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class.getName(),
        "schema.registry.url", "http://localhost:8081",
        "specific.protobuf.value.type", Person.class.getName() // Required config for Protobuf specifics
    ));

.exec(
    kafka("Protobuf Request")
        .requestReply()
        .requestTopic("proto-req")
        .responseTopic("proto-res")
        .key(session -> "key")
        // Use your generated Protobuf classes
        .value(session -> Person.newBuilder().setName("Bob").build())
        // Explicitly map it
        .serializationType(String.class, Person.class, SerializationType.PROTOBUF)
)
```

## Custom Objects

Gatling doesn't limit you. Provided you configure the appropriate Kafka `Serializer` implementations inside the `KafkaProtocolBuilder`, you can hand over pure POJOs right inside `.value()`.

```java
.producerProperties(Map.of(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomBinaryProtocolSerializer.class.getName()
))

.exec(
    kafka("Custom Protocol")
        .requestReply()
        // ...
        .value(session -> new CustomInternalDomainObject())
        .serializationType(byte[].class, byte[].class, SerializationType.BYTE_ARRAY) 
)
```

## Consume-Only Serialization Hints

When using the `KafkaDsl.consume("topic")` action, Gatling is not sending any message, so it needs a hint about what type of object to expect back from the Kafka Deserializer so that the subsequent `.check()` steps receive the correct generic type.

```java
import static pl.perfluencer.kafka.javaapi.KafkaDsl.*;

ScenarioBuilder scn = scenario("Consume Only")
    .exec(
        consume("my-topic")
            // Provide serialization hints to the checks compiler
            .asAvro(GenericRecord.class)
            // .asProtobuf(Person.class)
            // .asString()
            // .asBytes()
            
            .check(...)
    )
```
