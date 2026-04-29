/*
 * Copyright 2026 Perfluencer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.perfluencer.kafka.integration;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import java.util.Map;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;

public class BrokenAvroSimulation extends Simulation {

    private static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"User\","
            + "\"fields\":["
            + "  {\"name\":\"name\",\"type\":\"string\"},"
            + "  {\"name\":\"age\",\"type\":\"int\"}"
            + "]}";

    {
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers.e2e", "localhost:9092");
        String schemaRegistryUrl = System.getProperty("schema.registry.url.e2e", "http://localhost:8081");

        KafkaProtocolBuilder protocol = KafkaDsl.kafka()
                .bootstrapServers(bootstrapServers)
                .numConsumers(0)
                .producerProperties(Map.of(
                        "value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer",
                        "schema.registry.url", schemaRegistryUrl
                ));

        Schema schema = new Schema.Parser().parse(USER_SCHEMA);

        ScenarioBuilder scn = scenario("Broken Avro Validation")
                .exec(
                        KafkaDsl.kafka("Send Invalid Avro")
                                .send()
                                .topic("broken-avro-topic")
                                .key(session -> UUID.randomUUID().toString())
                                .value(session -> {
                                    GenericRecord user = new GenericData.Record(schema);
                                    
                                    // INTENTIONAL ERRORS:
                                    
                                    // 1. Wrong type: 'age' is expected to be 'int', but we provide a 'String'
                                    // Note: GenericData.Record.put() might not throw immediately, 
                                    // but KafkaAvroSerializer WILL fail during send.
                                    user.put("name", "BrokenUser");
                                    user.put("age", "this_is_not_a_number"); 
                                    
                                    // 2. Non-existent field (uncomment to see immediate crash in lambda)
                                    // user.put("unknown_field", "crash");
                                    
                                    return user;
                                })
                                .asAvro()
                );

        setUp(
                scn.injectOpen(atOnceUsers(1))
        ).protocols(protocol);
    }
}
