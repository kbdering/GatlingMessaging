package pl.perfluencer.kafka.integration;

import io.gatling.commons.stats.Status;
import io.gatling.core.CoreComponents;
import io.gatling.core.action.Action;
import io.gatling.core.stats.StatsEngine;
import org.junit.jupiter.api.Test;
import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.cache.RequestData;
import pl.perfluencer.kafka.MessageProcessor;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import io.gatling.commons.util.Clock;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import scala.Option;
import pl.perfluencer.common.util.SerializationType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class KafkaChecksWiringTest {

        @Test
        public void testChecksAreRegisteredAndExecuted() {
                // 1. Setup Data
                String requestName = "TestRequest";
                String successfulBody = "SUCCESS";
                String failureBody = "FAILURE";
                String correlationId = "123456789012345678901234";

                // 2. Mock Core Components
                CoreComponents coreComponents = mock(CoreComponents.class);
                Clock mockClock = mock(Clock.class);
                StatsEngine mockStatsEngine = mock(StatsEngine.class);
                when(coreComponents.clock()).thenReturn(mockClock);
                when(coreComponents.statsEngine()).thenReturn(mockStatsEngine);
                when(mockClock.nowMillis()).thenReturn(1000L);

                // 3. Setup Protocol with Real Registry
                KafkaProtocolBuilder.KafkaProtocol protocol = mock(KafkaProtocolBuilder.KafkaProtocol.class);
                ConcurrentMap<String, List<MessageCheck<?, ?>>> registry = new ConcurrentHashMap<>();

                // Mock getCheckRegistry to return our real map
                when(protocol.getCheckRegistry()).thenReturn(registry);

                // Handle registerChecks calls - forward to real map
                doAnswer(invocation -> {
                        String name = invocation.getArgument(0);
                        List<MessageCheck<?, ?>> checks = invocation.getArgument(1);
                        registry.put(name, checks);
                        return null;
                }).when(protocol).registerChecks(anyString(), anyList());

                // when(protocol.getRequestTimeout()).thenReturn(Duration.ofSeconds(1)); //
                // KafkaProtocol doesn't have getRequestTimeout directly exposed like MQ maybe?
                // Checking KafkaProtocol methods... pollTimeout, etc. Not critical for this
                // test setup if we mock correct things.

                // Mock ActorSystem for Action creation
                ActorSystem mockSystem = mock(ActorSystem.class);
                when(protocol.getActorSystem()).thenReturn(mockSystem);
                when(mockSystem.actorOf(any(Props.class))).thenReturn(mock(ActorRef.class));

                // Mock getConsumerAndProcessor
                when(protocol.getConsumerAndProcessor(anyString())).thenReturn(null);
                // We will manually register consumerAndProcessor if needed, but the builder
                // does it.

                // 4. Create a Check using public constructor
                // Kafka's MessageCheck might need to be created via builder or check imports
                // Checking MessageCheck.java in previous context... it has responseNotEmpty
                // static
                // Let's assume standard MessageCheck constructor exists or use factory
                MessageCheck<String, String> testCheck = new MessageCheck<>(
                                "Failure Check",
                                String.class,
                                SerializationType.STRING,
                                String.class,
                                SerializationType.STRING,
                                (req, resp) -> resp.contains("FAILURE") ? Optional.of("Body contained FAILURE")
                                                : Optional.empty());

                // 5. Simulate what ActionBuilder does: Register checks manually since we can't
                // easily run full builder in unit test without complex context
                // But wait, the goal is to test WIRING.
                // Let's manually register checks to simulate ActionBuilder
                protocol.registerChecks(requestName, Collections.singletonList(testCheck));

                // VERIFICATION: Check is registered
                assertTrue(registry.containsKey(requestName), "Registry should contain request check info");
                assertEquals(1, registry.get(requestName).size(), "Should have 1 registered check");

                // 6. Setup RequestStore and MessageProcessor
                RequestStore requestStore = new InMemoryRequestStore();
                MessageProcessor processor = new MessageProcessor(
                                requestStore,
                                mockStatsEngine,
                                mockClock,
                                null, // controller
                                registry, // Pass the registry!
                                null,
                                Collections.emptyList(), // sessionAwareChecks
                                null, // correlationExtractor
                                false, // useTimestampHeader
                                Duration.ofMillis(10), // retryBackoff
                                0 // maxRetries
                );

                // 7. Store a fake request
                long startTime = 1000L;
                // In Kafka, serialization type logic inside MessageProcessor checks looks at
                // CHECK's expected types.
                // But storeRequest needs to store it properly.
                requestStore.storeRequest(new RequestData(
                                correlationId,
                                "KEY", // key
                                "RequestPayload".getBytes(StandardCharsets.UTF_8),
                                SerializationType.BYTE_ARRAY,
                                requestName,
                                "Scenario",
                                startTime,
                                startTime + 1000,
                                null));

                // 8. Process a FAIL response
                // MessageProcessor.Input is List<ConsumerRecord>
                // We need to construct ConsumerRecord
                org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> failRecord = new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                                "RESPONSE_TOPIC",
                                0,
                                0L,
                                "KEY",
                                (Object) failureBody.getBytes(StandardCharsets.UTF_8));
                // Need to add header for correlation ID extraction?
                // The default correlation extractor (passed as null above) might fail if we
                // don't mock it or use a real one.
                // The test above passed `null` for correlationExtractor.
                // MessageProcessor logic:
                // if (correlationExtractor != null) { ... } else { ... }
                // If null, correlationId is null.
                // If correlationId is null, it skips lookup.

                // We MUST provide a correlation extractor!
                // Let's use HeaderExtractor logic or just mock it.
                pl.perfluencer.kafka.extractors.CorrelationExtractor mockExtractor = mock(
                                pl.perfluencer.kafka.extractors.CorrelationExtractor.class);
                when(mockExtractor.extract(any())).thenReturn(correlationId);

                // Re-create processor with extractor
                processor = new MessageProcessor(
                                requestStore,
                                mockStatsEngine,
                                mockClock,
                                null,
                                registry,
                                null,
                                Collections.emptyList(),
                                mockExtractor,
                                false,
                                Duration.ofMillis(10),
                                0);

                processor.process(Collections.singletonList(failRecord));

                // VERIFICATION: StatsEngine should log KO
                verify(mockStatsEngine).logResponse(
                                eq("Scenario"),
                                any(), // groups
                                eq(requestName),
                                eq(startTime),
                                anyLong(), // endTime
                                eq(Status.apply("KO")), // EXPECT KO
                                eq(Option.apply("500")),
                                argThat(msg -> msg.toString().contains("Body contained FAILURE")));

                // 9. Store another request for SUCCESS
                String corrId2 = "SUCCESS_ID_123456789012";
                when(mockExtractor.extract(argThat(rec -> rec.value().toString().contains(successfulBody)
                                || new String((byte[]) rec.value()).contains(successfulBody)))).thenReturn(corrId2);

                requestStore.storeRequest(new RequestData(
                                corrId2,
                                "KEY",
                                "RequestPayload".getBytes(StandardCharsets.UTF_8),
                                SerializationType.BYTE_ARRAY,
                                requestName,
                                "Scenario",
                                startTime,
                                startTime + 1000,
                                null));

                // 10. Process a SUCCESS response
                org.apache.kafka.clients.consumer.ConsumerRecord<String, Object> successRecord = new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                                "RESPONSE_TOPIC",
                                0,
                                1L,
                                "KEY",
                                (Object) successfulBody.getBytes(StandardCharsets.UTF_8));

                // Reset stats engine mock
                reset(mockStatsEngine);
                processor.process(Collections.singletonList(successRecord));

                // VERIFICATION: StatsEngine should log OK
                verify(mockStatsEngine).logResponse(
                                eq("Scenario"),
                                any(),
                                eq(requestName),
                                eq(startTime),
                                anyLong(),
                                eq(Status.apply("OK")), // EXPECT OK
                                eq(Option.apply("200")),
                                eq(Option.empty()));
        }
}
