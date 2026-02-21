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

import io.gatling.app.Gatling;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaMetricInjectionTest {

    public static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @BeforeClass
    public static void setup() {
        kafka.start();
        System.setProperty("kafka.bootstrap.servers", kafka.getBootstrapServers());
    }

    @AfterClass
    public static void teardown() {
        kafka.stop();
        System.clearProperty("kafka.bootstrap.servers");
    }

    @Test
    public void testMetricInjection() {

        String[] args = new String[] {
                "--simulation", pl.perfluencer.kafka.simulations.MetricInjectionSimulation.class.getName(),
                "--results-folder", "target/gatling"
        };
        // Use fromArgs to avoid System.exit
        Gatling.main(args);
    }
}
