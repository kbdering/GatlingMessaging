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

package pl.perfluencer.kafka.actors;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.ActorRef;
import io.gatling.core.CoreComponents;
import io.gatling.core.stats.StatsEngine;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import pl.perfluencer.kafka.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.List;

public class MessageProcessorActor extends AbstractActor {

        private final MessageProcessor messageProcessor;

        public MessageProcessorActor(RequestStore requestStore, CoreComponents coreComponents,
                        java.util.concurrent.ConcurrentMap<String, List<MessageCheck<?, ?>>> checkRegistry,
                        java.util.List<MessageCheck<?, ?>> globalChecks,
                        CorrelationExtractor correlationExtractor,
                        boolean useTimestampHeader, Duration retryBackoff, int maxRetries,
                        String leakageScenarioName) {
                this.messageProcessor = new MessageProcessor(
                                requestStore,
                                coreComponents.statsEngine(),
                                coreComponents.clock(),
                                null, // Controller is not compatible with Pekko ActorRef in newer Gatling versions
                                checkRegistry,
                                globalChecks,
                                null,
                                correlationExtractor,
                                useTimestampHeader,
                                retryBackoff,
                                maxRetries,
                                null,
                                MessageProcessor.E2E_GROUP_COLLECTION(), // Default group
                                leakageScenarioName);
        }

        // Constructor for testing
        public MessageProcessorActor(RequestStore requestStore, StatsEngine statsEngine,
                        io.gatling.commons.util.Clock clock,
                        java.util.concurrent.ConcurrentMap<String, List<MessageCheck<?, ?>>> checkRegistry,
                        CorrelationExtractor correlationExtractor,
                        boolean useTimestampHeader) {
                this.messageProcessor = new MessageProcessor(requestStore, statsEngine, clock, null, checkRegistry,
                                null, null,
                                correlationExtractor,
                                useTimestampHeader, Duration.ofMillis(50), 3, null, MessageProcessor.E2E_GROUP_COLLECTION(),
                                "Gatling Data Leakage Audit");
        }

        public static Props props(RequestStore requestStore, CoreComponents coreComponents,
                        java.util.concurrent.ConcurrentMap<String, List<MessageCheck<?, ?>>> checkRegistry,
                        java.util.List<MessageCheck<?, ?>> globalChecks,
                        CorrelationExtractor correlationExtractor, boolean useTimestampHeader,
                        Duration retryBackoff, int maxRetries, String leakageScenarioName) {
                // Ensure dependencies are non-null if required
                java.util.Objects.requireNonNull(requestStore, "RequestStore cannot be null");
                java.util.Objects.requireNonNull(coreComponents, "CoreComponents cannot be null");
                return Props.create(MessageProcessorActor.class,
                                () -> new MessageProcessorActor(requestStore, coreComponents, checkRegistry,
                                                globalChecks,
                                                correlationExtractor,
                                                useTimestampHeader,
                                                retryBackoff, maxRetries, leakageScenarioName));
        }

        public static Props props(RequestStore requestStore, CoreComponents coreComponents,
                        java.util.concurrent.ConcurrentMap<String, List<MessageCheck<?, ?>>> checkRegistry,
                        String correlationHeaderName) {
                // Default legacy fallback for tests
                return props(requestStore, coreComponents, checkRegistry, null,
                                new pl.perfluencer.kafka.extractors.HeaderExtractor(correlationHeaderName), false,
                                Duration.ofMillis(50), 3, "Gatling Data Leakage Audit");
        }

        public static class RetryBatch {
                public final List<MessageProcessor.RetryEnvelope> envelopes;

                public RetryBatch(List<MessageProcessor.RetryEnvelope> envelopes) {
                        this.envelopes = envelopes;
                }
        }

        @Override
        public Receive createReceive() {
                return receiveBuilder()
                                .match(org.apache.kafka.clients.consumer.ConsumerRecords.class, records -> {
                                        // Direct processing of ConsumerRecords - avoids ArrayList allocation
                                        @SuppressWarnings("unchecked")
                                        org.apache.kafka.clients.consumer.ConsumerRecords<String, Object> typedRecords = (org.apache.kafka.clients.consumer.ConsumerRecords<String, Object>) records;
                                        List<MessageProcessor.RetryEnvelope> retries = messageProcessor
                                                        .process(typedRecords);
                                        scheduleRetries(retries);
                                })
                                .match(List.class, list -> {
                                        // Legacy: processing of List<ConsumerRecord> (for backwards compatibility)
                                        @SuppressWarnings("unchecked")
                                        List<ConsumerRecord<String, Object>> records = (List<ConsumerRecord<String, Object>>) list;
                                        List<MessageProcessor.RetryEnvelope> retries = messageProcessor
                                                        .process(records);
                                        scheduleRetries(retries);
                                })
                                .match(RetryBatch.class, msg -> {
                                        // Processing of retries
                                        List<MessageProcessor.RetryEnvelope> retries = messageProcessor
                                                        .processRetries(msg.envelopes);
                                        scheduleRetries(retries);
                                })
                                .build();
        }

        private void scheduleRetries(List<MessageProcessor.RetryEnvelope> retries) {
                if (!retries.isEmpty()) {
                        getContext().system().scheduler().scheduleOnce(
                                        messageProcessor.getRetryBackoff(),
                                        self(),
                                        new RetryBatch(retries),
                                        getContext().dispatcher(),
                                        self());
                }
        }
}
