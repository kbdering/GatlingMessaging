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

import io.gatling.commons.stats.Status;
import io.gatling.commons.util.Clock;
import io.gatling.core.stats.StatsEngine;

import pl.perfluencer.kafka.extractors.HeaderExtractor;
import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.common.util.SerializationType;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.MessageProcessor;
import pl.perfluencer.cache.RequestData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.pekko.actor.ActorRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Validates that the groups argument passed to StatsEngine.logResponse()
 * is correct for both the E2E metric (should use the stored statsGroup)
 * and does NOT trigger any logGroupEnd calls.
 *
 * Regression test for:
 *   - NoSuchElementException: key not found: 0 (caused by fake logGroupEnd)
 *   - ACK "send" metric must be ungrouped (empty list)
 *   - E2E metric must use the processor's configured statsGroup
 */
public class GroupMetricsValidationTest {

    private InMemoryRequestStore requestStore;
    private GroupCapturingStatsEngine statsEngine;
    private MessageProcessor processor;

    @Before
    public void setUp() {
        requestStore = new InMemoryRequestStore();
        statsEngine = new GroupCapturingStatsEngine();

        Clock clock = System::currentTimeMillis;

        // Configure the processor with a known statsGroup so we can assert it
        scala.collection.immutable.List<String> statsGroup = scala.collection.immutable.List$.MODULE$
                .from(scala.jdk.javaapi.CollectionConverters.asScala(
                        java.util.List.of("E2E Group")));

        processor = new MessageProcessor(
                requestStore,
                statsEngine,
                clock,
                ActorRef.noSender(),
                new java.util.concurrent.ConcurrentHashMap<>(),
                Collections.emptyList(),  // globalChecks
                null,                     // sessionAwareChecks
                new HeaderExtractor("correlationId"),
                false,                    // useTimestampHeader
                java.time.Duration.ofMillis(10),
                0,
                null,                     // parserRegistry
                statsGroup,
                "Leakage Scenario"
        );
    }

    @After
    public void tearDown() throws Exception {
        requestStore.close();
    }

    /**
     * Verifies that when a correlated response arrives, the E2E logResponse call
     * uses the groups stored in RequestData (the capture groups from the original session),
     * and no logGroupEnd is invoked at all.
     */
    @Test
    public void testE2EMetricUsesSessionGroupsFromRequestData() throws InterruptedException {
        String correlationId = "grp-test-corr-1";
        String scenarioName = "GroupValidationScenario";
        String requestName = "Place Order";
        
        // Define some specific groups we want to see preserved
        scala.collection.immutable.List<String> sessionGroups = scala.collection.immutable.List$.MODULE$
                .from(scala.jdk.javaapi.CollectionConverters.asScala(
                        java.util.List.of("Parent Group", "Sub Group")));

        // Store a pending request WITH captured session groups
        requestStore.storeRequest(new RequestData(
                correlationId, "key-1", "payload".getBytes(),
                SerializationType.STRING,
                requestName, scenarioName,
                System.currentTimeMillis(), 30_000L, null,
                sessionGroups
        ));

        // Simulate correlated response arriving
        ConsumerRecord<String, Object> response = buildResponseRecord(correlationId, "response-payload");
        processor.process(Collections.singletonList(response));

        // Allow async batch processor to complete
        Thread.sleep(200);

        // --- Assertions ---

        // 1. At least one logResponse call should have been made
        assertFalse("StatsEngine.logResponse should have been called",
                statsEngine.capturedCalls.isEmpty());

        // 2. Find the E2E entry
        GroupCapturingStatsEngine.LoggedResponse e2eCall = statsEngine.capturedCalls.stream()
                .filter(c -> c.requestName.equals(requestName))
                .findFirst()
                .orElse(null);

        assertNotNull("E2E logResponse call not found", e2eCall);

        // 3. E2E MUST use the groups captured in RequestData
        assertEquals("E2E metric must be logged with the captured session groups",
                java.util.List.of("Parent Group", "Sub Group"),
                e2eCall.groups);

        // 4. No logGroupEnd should ever be called
        assertEquals("logGroupEnd must NEVER be called by MessageProcessor",
                0, statsEngine.logGroupEndCallCount);
    }

    /**
     * Verifies that an unmatched response (no pending request in store)
     * also uses the configured statsGroup and does not call logGroupEnd.
     */
    @Test
    public void testUnmatchedResponseUsesConfiguredStatsGroup() {
        ConsumerRecord<String, Object> response = buildResponseRecord("nonexistent-corr-id", "orphan-payload");
        processor.process(Collections.singletonList(response));

        assertFalse("StatsEngine should record something even for unmatched",
                statsEngine.capturedCalls.isEmpty());

        GroupCapturingStatsEngine.LoggedResponse call = statsEngine.capturedCalls.get(0);
        assertEquals("Unmatched response should be KO", "KO", call.status.name());
        assertEquals("Unmatched response must use statsGroup",
                java.util.List.of("E2E Group"), call.groups);

        assertEquals("logGroupEnd must NEVER be called", 0, statsEngine.logGroupEndCallCount);
    }

    /**
     * Verifies that multiple concurrent requests all correctly report
     * under the statsGroup without any logGroupEnd calls.
     */
    @Test
    public void testConcurrentRequestsNeverCallLogGroupEnd() throws InterruptedException {
        int count = 10;
        for (int i = 0; i < count; i++) {
            String corrId = "concurrent-" + i;
            requestStore.storeRequest(new RequestData(
                    corrId, "key-" + i, ("val-" + i).getBytes(),
                    SerializationType.STRING,
                    "Concurrent Request", "ConcurrentScenario",
                    System.currentTimeMillis(), 30_000L, null
            ));
        }

        List<ConsumerRecord<String, Object>> batch = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            batch.add(buildResponseRecord("concurrent-" + i, "resp-" + i));
        }

        processor.process(batch);
        Thread.sleep(300);

        assertEquals("logGroupEnd must NEVER be called regardless of concurrency",
                0, statsEngine.logGroupEndCallCount);

        long e2eOkCount = statsEngine.capturedCalls.stream()
                .filter(c -> c.requestName.equals("Concurrent Request") && c.status.name().equals("OK"))
                .count();
        assertEquals("All " + count + " E2E calls should be OK", count, e2eOkCount);
    }

    // ─── Helpers ───────────────────────────────────────────────────────────────

    private static ConsumerRecord<String, Object> buildResponseRecord(String correlationId, String value) {
        ConsumerRecord<String, Object> rec = new ConsumerRecord<>(
                "response-topic", 0, 0L,
                "key", (Object) value.getBytes(StandardCharsets.UTF_8));
        rec.headers().add(new RecordHeader("correlationId",
                correlationId.getBytes(StandardCharsets.UTF_8)));
        return rec;
    }

    /**
     * A test-double StatsEngine that captures every logResponse call
     * and counts logGroupEnd invocations so tests can assert on both.
     */
    static class GroupCapturingStatsEngine implements StatsEngine {
        final CopyOnWriteArrayList<LoggedResponse> capturedCalls = new CopyOnWriteArrayList<>();
        volatile int logGroupEndCallCount = 0;

        static class LoggedResponse {
            final String scenario;
            final List<String> groups;
            final String requestName;
            final Status status;

            LoggedResponse(String scenario, List<String> groups, String requestName, Status status) {
                this.scenario = scenario;
                this.groups = groups;
                this.requestName = requestName;
                this.status = status;
            }

            @Override
            public String toString() {
                return "[" + scenario + " | groups=" + groups + " | " + requestName + " | " + status.name() + "]";
            }
        }

        @Override
        public void logResponse(String scenario, scala.collection.immutable.List<String> groups,
                                String requestName, long startTimestamp, long endTimestamp,
                                Status status, scala.Option<String> responseCode,
                                scala.Option<String> message) {
            // Convert Scala List → Java List for easy assertion
            List<String> javaGroups = scala.jdk.javaapi.CollectionConverters.asJava(groups);
            capturedCalls.add(new LoggedResponse(scenario, javaGroups, requestName, status));
        }

        @Override
        public void logGroupEnd(String scenario,
                                io.gatling.core.session.GroupBlock groupBlock,
                                long exitTimestamp) {
            logGroupEndCallCount++;
        }

        @Override public void logUserStart(String scenario) {}
        @Override public void logUserEnd(String scenario) {}
        @Override public void logRequestCrash(String scenario, scala.collection.immutable.List<String> groups,
                                               String requestName, String error) {}
        @Override public void start() {}
        @Override public void stop(io.gatling.core.actor.ActorRef<io.gatling.core.controller.Controller.Command> controller,
                                   scala.Option<Exception> exception) {}
    }
}
