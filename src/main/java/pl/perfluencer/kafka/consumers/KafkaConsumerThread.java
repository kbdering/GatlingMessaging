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

package pl.perfluencer.kafka.consumers;

import io.gatling.commons.stats.Status;
import io.gatling.core.stats.StatsEngine;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.perfluencer.kafka.MessageProcessor;
import scala.Option;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-based Kafka consumer that replaces the actor-based KafkaConsumerActor.
 * Supports both synchronous and asynchronous offset commits.
 */
public class KafkaConsumerThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerThread.class);

    private final org.apache.kafka.clients.consumer.Consumer<String, Object> consumer;
    private final MessageProcessor messageProcessor;
    private final Duration pollDuration;
    private final StatsEngine statsEngine;
    private final String responseTopic;
    private final boolean syncCommit;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final scala.collection.immutable.List<String> statsGroup; // Added field

    // Consume-only mode fields (no correlation, just apply checks to every record)
    private final String consumeOnlyRequestName;
    private final String consumeOnlyScenarioName;

    public KafkaConsumerThread(Map<String, Object> consumerProperties, String responseTopic,
            MessageProcessor messageProcessor, StatsEngine statsEngine,
            Duration pollDuration, boolean syncCommit, int threadId) {
        this(consumerProperties, responseTopic, messageProcessor, statsEngine, pollDuration, syncCommit, threadId,
                null, null, scala.collection.immutable.List$.MODULE$.empty()); // Updated call
    }

    /**
     * Constructor for consume-only mode. When consumeOnlyRequestName is non-null,
     * the thread calls {@code messageProcessor.processConsumeOnly()} instead of
     * the correlation-based {@code process()}.
     */
    public KafkaConsumerThread(Map<String, Object> consumerProperties, String responseTopic,
            MessageProcessor messageProcessor, StatsEngine statsEngine,
            Duration pollDuration, boolean syncCommit, int threadId,
            String consumeOnlyRequestName, String consumeOnlyScenarioName) {
        this(consumerProperties, responseTopic, messageProcessor, statsEngine, pollDuration, syncCommit, threadId,
                consumeOnlyRequestName, consumeOnlyScenarioName, scala.collection.immutable.List$.MODULE$.empty()); // Updated
                                                                                                                    // call
    }

    // New constructor with statsGroup
    public KafkaConsumerThread(Map<String, Object> consumerProperties, String responseTopic,
            MessageProcessor messageProcessor, StatsEngine statsEngine,
            Duration pollDuration, boolean syncCommit, int threadId,
            String consumeOnlyRequestName, String consumeOnlyScenarioName,
            scala.collection.immutable.List<String> statsGroup) {
        super("KafkaConsumer-" + responseTopic + "-" + threadId);
        setDaemon(true);
        this.consumeOnlyRequestName = consumeOnlyRequestName;
        this.consumeOnlyScenarioName = consumeOnlyScenarioName;
        this.statsGroup = statsGroup; // Initialize new field

        logger.debug("Starting KafkaConsumerThread for topic: {} with group: {} (syncCommit={})",
                responseTopic, consumerProperties.get("group.id"), syncCommit);

        if (!consumerProperties.containsKey("auto.offset.reset")) {
            logger.warn(
                    "auto.offset.reset is not set. Defaulting to 'latest' which may cause missed messages in tests.");
        }

        try {
            this.consumer = new KafkaConsumer<>(consumerProperties);
        } catch (Exception e) {
            logger.error("Failed to create Kafka Consumer. Check your bootstrap.servers and group.id configuration.",
                    e);
            throw e;
        }

        this.messageProcessor = messageProcessor;
        this.pollDuration = pollDuration;
        this.statsEngine = statsEngine;
        this.responseTopic = responseTopic;
        this.syncCommit = syncCommit;
        consumer.subscribe(Collections.singletonList(responseTopic));
    }

    // Visible for testing
    public KafkaConsumerThread(org.apache.kafka.clients.consumer.Consumer<String, Object> consumer,
            String responseTopic, MessageProcessor messageProcessor, StatsEngine statsEngine,
            Duration pollDuration, boolean syncCommit, int threadId) {
        super("KafkaConsumer-" + responseTopic + "-" + threadId);
        setDaemon(true);

        this.consumer = consumer;
        this.messageProcessor = messageProcessor;
        this.pollDuration = pollDuration;
        this.statsEngine = statsEngine;
        this.responseTopic = responseTopic;
        this.syncCommit = syncCommit;
        this.consumeOnlyRequestName = null;
        this.consumeOnlyScenarioName = null;
        this.statsGroup = scala.collection.immutable.List$.MODULE$.empty(); // Initialize new field for testing
                                                                            // constructor
        consumer.subscribe(Collections.singletonList(responseTopic));
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                poll();
            }
        } finally {
            closeConsumer();
        }
    }

    private void poll() {
        try {
            ConsumerRecords<String, Object> records = consumer.poll(pollDuration);
            logger.debug("KafkaConsumerThread [{}] polled {} records from topic {}", this.getId(),
                    records.count(),
                    responseTopic);
            if (!records.isEmpty()) {
                // Process records asynchronously - don't block the poll loop
                java.util.concurrent.CompletableFuture.runAsync(() -> {
                    try {
                        if (consumeOnlyRequestName != null) {
                            // Consume-only mode: apply response-only checks, no correlation
                            messageProcessor.processConsumeOnly(records,
                                    consumeOnlyRequestName, consumeOnlyScenarioName);
                        } else {
                            // Request-reply mode: correlate with pending requests
                            messageProcessor.process(records);
                        }
                    } catch (Exception e) {
                        logger.error("Error processing records", e);
                    }
                });

                // Commit offsets based on configuration
                if (syncCommit) {
                    commitSync();
                } else {
                    commitAsync();
                }
            }
        } catch (WakeupException e) {
            // Expected during shutdown
            if (running.get()) {
                logger.warn("Unexpected WakeupException while running", e);
            }
        } catch (Exception e) {
            logger.error("Error polling Kafka", e);
            // Backoff on error to prevent tight loop
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void commitSync() {
        long commitStartTime = System.currentTimeMillis();
        try {
            consumer.commitSync();
        } catch (CommitFailedException e) {
            logger.debug("Sync offset commit failed", e);
            reportCommitFailure(commitStartTime, e);
        } catch (Exception e) {
            logger.debug("Unexpected error during sync commit", e);
            reportCommitFailure(commitStartTime, e);
        }
    }

    private void commitAsync() {
        final long commitStartTime = System.currentTimeMillis();
        consumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                logger.debug("Async offset commit failed", exception);
                reportCommitFailure(commitStartTime, exception);
            }
        });
    }

    private void reportCommitFailure(long startTime, Exception exception) {
        if (statsEngine != null) {
            long endTime = System.currentTimeMillis();
            statsEngine.logResponse(
                    responseTopic, // scenario name
                    statsGroup, // groups
                    "KafkaOffsetCommit", // transaction name
                    startTime, // start time
                    endTime, // end time
                    Status.apply("KO"), // status
                    Option.apply("COMMIT_FAILED"), // response code
                    Option.apply(exception.getMessage())); // error message
        }
    }

    public void shutdown() {
        running.set(false);
        consumer.wakeup();
    }

    private void closeConsumer() {
        try {
            consumer.close();
            logger.info("Kafka consumer closed for topic: {}", responseTopic);
        } catch (Exception e) {
            logger.error("Error closing Kafka consumer", e);
        }
    }

    public boolean isRunning() {
        return running.get();
    }
}
