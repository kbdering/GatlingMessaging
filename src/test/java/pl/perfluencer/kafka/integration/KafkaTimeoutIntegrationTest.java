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
