# Multi-Protocol Execution

Most systems today communicate using a standardized serialization format. However, as organizations migrate from JSON over to Avro (or Schema Registry), or if you are load testing a massive central nervous Kafka cluster, you may need a single Gatling simulation to blast traffic using completely disparate settings. 

The Gatling Kafka extension supports defining **multiple discrete `KafkaProtocol` instances** within a single simulation script.

## The Principle Limit

To achieve maximum performance mapping under load and latency resolution down to microseconds, **each protocol instance is restricted to a specific base configuration**.

Instead of trying to override Serializers inside an internal `.exec()`, you instantiate multiple `KafkaProtocol` builders up-front, and firmly map them to their corresponding `ScenarioBuilders` during the final `.setUp()` phase.

## Example: Avro & JSON Simultaneous Load

```java
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

// Protocol 1: String / JSON Configuration
KafkaProtocolBuilder jsonProtocol = KafkaDsl.kafka()
    .bootstrapServers("kafka:9092")
    // Note: Give multi-protocols unique Consumer Group IDs
    .groupId("gatling-json-group") 
    .producerProperties(Map.of(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
    ));

// Protocol 2: Avro Schema Registry Configuration
KafkaProtocolBuilder avroProtocol = KafkaDsl.kafka()
    .bootstrapServers("kafka:9092")
    .groupId("gatling-avro-group")
    .producerProperties(Map.of(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName(),
        "schema.registry.url", "http://schema-registry:8081"
    ));

// Scenario 1: Focuses completely on JSON flows
ScenarioBuilder jsonScenario = scenario("JSON Flow")
    .exec(
        kafka("Send JSON")
            .requestReply()
            .requestTopic("json-in")
            // ...
    );

// Scenario 2: Focuses completely on Avro flows
ScenarioBuilder avroScenario = scenario("Avro Flow")
    .exec(
        kafka("Send Avro")
            .requestReply()
            .requestTopic("avro-in")
            // ...
    );

// Setup blocks mapping Protocols to Scenarios!
setUp(
    // Map the JSON scenario to the String Serializer Protocol
    jsonScenario.injectOpen(atOnceUsers(10)).protocols(jsonProtocol),
    
    // Map the Avro scenario to the Schema Registry Protocol
    avroScenario.injectOpen(atOnceUsers(10)).protocols(avroProtocol)
);
```

### When to separate?
You MUST instantiate a new Protocol if you need to:
- Talk to a different Kafka Cluster (`bootstrapServers`).
- Use different Authentication/TLS parameters.
- Talk to a Schema Registry.
- Use a different Producer or Consumer Serializer.
- Use a distinct `RequestStore` configuration.
