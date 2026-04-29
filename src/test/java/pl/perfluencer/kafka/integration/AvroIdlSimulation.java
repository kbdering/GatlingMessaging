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
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

import static io.gatling.javaapi.core.CoreDsl.*;

public class AvroIdlSimulation extends Simulation {

    private Schema loadSchemaFromIdl() {
        try {
            URL resource = getClass().getClassLoader().getResource("user.avdl");
            if (resource == null) {
                throw new RuntimeException("user.avdl not found in classpath");
            }
            File idlFile = new File(resource.getFile());
            try (Idl parser = new Idl(idlFile)) {
                org.apache.avro.Protocol protocol = parser.CompilationUnit();
                return protocol.getType("User");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load schema from IDL", e);
        }
    }

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

        Schema schema = loadSchemaFromIdl();

        ScenarioBuilder scn = scenario("Avro from IDL File")
                .exec(
                        KafkaDsl.kafka("Send User from IDL")
                                .send()
                                .topic("avro-idl-topic")
                                .key(session -> UUID.randomUUID().toString())
                                .value(session -> {
                                    GenericRecord user = new GenericData.Record(schema);
                                    user.put("name", "IdlUser");
                                    user.put("age", 28);
                                    return user;
                                })
                                .asAvro()
                );

        setUp(
                scn.injectOpen(atOnceUsers(5))
        ).protocols(protocol);
    }
}
