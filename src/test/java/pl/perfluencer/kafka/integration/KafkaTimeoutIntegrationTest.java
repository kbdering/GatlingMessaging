package pl.perfluencer.kafka.integration;

import pl.perfluencer.kafka.actions.KafkaRequestReplyActionBuilder;
import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.kafka.javaapi.KafkaDsl;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import io.gatling.core.stats.StatsEngine;
import io.gatling.core.session.Session;
import io.gatling.core.CoreComponents;
import io.gatling.core.action.Action;
import io.gatling.commons.stats.Status;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

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
        InMemoryRequestStore requestStore = new InMemoryRequestStore(100); // 100ms check interval

        // Manually trigger timeout monitoring
        CountDownLatch timeoutLatch = new CountDownLatch(1);
        requestStore.startTimeoutMonitoring((correlationId, requestData) -> {
            if ("corr-1".equals(correlationId)) {
                timeoutLatch.countDown();
            }
        });

        // Store a request with a short timeout
        long startTime = System.currentTimeMillis();
        requestStore.storeRequest("corr-1", "key", "value", null, "request", "scenario", startTime, 200); // 200ms
                                                                                                          // timeout

        // Wait for timeout to happen
        if (!timeoutLatch.await(2, TimeUnit.SECONDS)) {
            throw new RuntimeException("Timeout did not occur within 2 seconds");
        }

        requestStore.close();
    }
}
