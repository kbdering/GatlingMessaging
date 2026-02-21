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

import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.cache.RequestData;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class KafkaTimeoutIntegrationTest {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
    }

    @Test
    public void testRequestTimeout() throws Exception {
        // Create RequestStore with short check interval
        try (InMemoryRequestStore requestStore = new InMemoryRequestStore(100)) { // 100ms check interval
            // Manually trigger timeout monitoring
            CountDownLatch timeoutLatch = new CountDownLatch(1);
            requestStore.startTimeoutMonitoring((correlationId, requestData) -> {
                if ("corr-1".equals(correlationId)) {
                    timeoutLatch.countDown();
                }
            });

            // Store a request with a short timeout
            long startTime = System.currentTimeMillis();
            // 200ms timeout
            requestStore.storeRequest(new RequestData(
                    "corr-1", "key", "value", null, "request", "scenario", startTime, 200, null));

            // Wait for timeout to happen
            if (!timeoutLatch.await(2, TimeUnit.SECONDS)) {
                throw new RuntimeException("Timeout did not occur within 2 seconds");
            }
        }
    }
}
