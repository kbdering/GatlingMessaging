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

package pl.perfluencer.kafka.simulations;

import io.gatling.app.Gatling;

import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;

public class JmxReportingRunner {

    @Test
    public void runSimulation() {
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))) {
            kafka.start();

            // Pass bootstrap servers to the Simulation via System Property
            System.setProperty("kafka.bootstrap.servers", kafka.getBootstrapServers());

            // Run Gatling programmatically using standard args
            String[] args = new String[] {
                    "-s", "pl.perfluencer.kafka.simulation.JmxReportingSimulation",
                    "-rf", "target/gatling-reports",
                    "-bdf", "target/test-classes" // binaries directory
            };

            Gatling.main(args);
        }
    }
}
