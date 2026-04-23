package pl.perfluencer.kafka;

import pl.perfluencer.kafka.consumers.KafkaConsumerThread;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the consumer readiness synchronization mechanism:
 * - awaitReady() signaling via ConsumerRebalanceListener
 * - seekToEnd behavior on partition assignment
 * - timeout behavior when no partition is assigned
 *
 * Note: The Confluent {@code MockConsumer} does not invoke
 * {@code ConsumerRebalanceListener} on {@code rebalance()}, so these tests
 * use a thin wrapper that captures and fires the listener for verification.
 */
public class KafkaConsumerThreadReadinessTest {

    /**
     * A thin wrapper around MockConsumer that captures the ConsumerRebalanceListener
     * registered via subscribe(), and exposes a method to trigger it manually.
     * This is necessary because the Confluent MockConsumer does not invoke the
     * listener on rebalance().
     */
    private static class ListenerCapturingMockConsumer extends MockConsumer<String, Object> {
        private ConsumerRebalanceListener capturedListener;

        ListenerCapturingMockConsumer() {
            super(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);
        }

        @Override
        public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            this.capturedListener = listener;
            super.subscribe(topics, listener);
        }

        /**
         * Simulates a rebalance that fires the listener, mimicking what a real
         * KafkaConsumer does during poll().
         */
        public void rebalanceWithListener(Collection<TopicPartition> partitions) {
            super.rebalance(new java.util.ArrayList<>(partitions));
            if (capturedListener != null) {
                capturedListener.onPartitionsAssigned(partitions);
            }
        }
    }

    /**
     * awaitReady() should return true once the consumer is assigned partitions
     * via a rebalance triggered by the first poll().
     */
    @Test
    public void testAwaitReadySucceeds() throws Exception {
        String topic = "test-topic";
        ListenerCapturingMockConsumer mockConsumer = new ListenerCapturingMockConsumer();
        TopicPartition tp = new TopicPartition(topic, 0);

        // Schedule rebalance to trigger onPartitionsAssigned
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalanceWithListener(Collections.singletonList(tp));
            mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        });

        MessageProcessor mockProcessor = mock(MessageProcessor.class);

        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                mockConsumer, topic, mockProcessor, null, Duration.ofMillis(100), false, 0);
        consumerThread.start();

        try {
            // awaitReady should return true within a reasonable timeout
            assertTrue("Consumer should become ready after partition assignment",
                    consumerThread.awaitReady(Duration.ofSeconds(5)));
            assertTrue("isReady() should return true", consumerThread.isReady());
        } finally {
            consumerThread.shutdown();
            consumerThread.join(1000);
        }
    }

    /**
     * awaitReady() should return false when no partition assignment occurs
     * within the timeout period.
     */
    @Test
    public void testAwaitReadyTimeout() throws Exception {
        String topic = "test-topic";
        MockConsumer<String, Object> mockConsumer = new MockConsumer<>(
                org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);

        // Do NOT schedule rebalance — the consumer will never get assigned partitions

        MessageProcessor mockProcessor = mock(MessageProcessor.class);

        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                mockConsumer, topic, mockProcessor, null, Duration.ofMillis(100), false, 0);
        consumerThread.start();

        try {
            // awaitReady should return false after timeout
            assertFalse("Consumer should NOT become ready without partition assignment",
                    consumerThread.awaitReady(Duration.ofMillis(500)));
            assertFalse("isReady() should return false", consumerThread.isReady());
        } finally {
            consumerThread.shutdown();
            consumerThread.join(1000);
        }
    }

    /**
     * When seekToEndOnReady is false (default), the consumer should become ready
     * but should NOT call seekToEnd on the consumer. We verify this by checking
     * that existing records at offset 0 can still be polled.
     */
    @Test
    public void testSeekToEndDisabledByDefault() throws Exception {
        String topic = "test-topic";
        ListenerCapturingMockConsumer mockConsumer = new ListenerCapturingMockConsumer();
        TopicPartition tp = new TopicPartition(topic, 0);

        CountDownLatch recordsProcessed = new CountDownLatch(1);
        MessageProcessor mockProcessor = mock(MessageProcessor.class);
        doAnswer(invocation -> {
            ConsumerRecords<?, ?> records = invocation.getArgument(0);
            if (!records.isEmpty()) {
                recordsProcessed.countDown();
            }
            return null;
        }).when(mockProcessor).process(any(ConsumerRecords.class));

        // Schedule rebalance with a pre-existing record
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalanceWithListener(Collections.singletonList(tp));
            mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            mockConsumer.addRecord(
                    new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 0, "key", "existing-value"));
        });

        // seekToEndOnReady = false (default)
        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                mockConsumer, topic, mockProcessor, null, Duration.ofMillis(100), false, 0, false);
        consumerThread.start();

        try {
            assertTrue("Consumer should become ready",
                    consumerThread.awaitReady(Duration.ofSeconds(5)));
            // The existing record should be processed (not seeked past)
            assertTrue("Pre-existing record should be processed",
                    recordsProcessed.await(5, TimeUnit.SECONDS));
        } finally {
            consumerThread.shutdown();
            consumerThread.join(1000);
        }
    }

    /**
     * When seekToEndOnReady is true, the consumer should seekToEnd on its
     * assigned partitions during the rebalance callback. As a result, pre-existing
     * records should be skipped.
     */
    @Test
    public void testSeekToEndEnabled() throws Exception {
        String topic = "test-topic";
        ListenerCapturingMockConsumer mockConsumer = new ListenerCapturingMockConsumer();
        TopicPartition tp = new TopicPartition(topic, 0);

        MessageProcessor mockProcessor = mock(MessageProcessor.class);

        // Schedule rebalance with a pre-existing record
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalanceWithListener(Collections.singletonList(tp));
            mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            mockConsumer.updateEndOffsets(Collections.singletonMap(tp, 1L));
            mockConsumer.addRecord(
                    new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 0, "key", "old-value"));
        });

        // seekToEndOnReady = true
        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                mockConsumer, topic, mockProcessor, null, Duration.ofMillis(100), false, 0, true);
        consumerThread.start();

        try {
            assertTrue("Consumer should become ready",
                    consumerThread.awaitReady(Duration.ofSeconds(5)));

            // Give the consumer some time to poll (if the old record were to be processed,
            // it would happen quickly)
            Thread.sleep(500);

            // The pre-existing record should NOT be processed because seekToEnd skipped it
            verify(mockProcessor, never()).process(any(ConsumerRecords.class));
        } finally {
            consumerThread.shutdown();
            consumerThread.join(1000);
        }
    }

    /**
     * awaitReady() should be idempotent — calling it multiple times after the
     * consumer is ready should always return true immediately.
     */
    @Test
    public void testAwaitReadyIdempotent() throws Exception {
        String topic = "test-topic";
        ListenerCapturingMockConsumer mockConsumer = new ListenerCapturingMockConsumer();
        TopicPartition tp = new TopicPartition(topic, 0);

        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalanceWithListener(Collections.singletonList(tp));
            mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        });

        MessageProcessor mockProcessor = mock(MessageProcessor.class);

        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                mockConsumer, topic, mockProcessor, null, Duration.ofMillis(100), false, 0);
        consumerThread.start();

        try {
            assertTrue(consumerThread.awaitReady(Duration.ofSeconds(5)));
            // Second call should return true immediately
            assertTrue(consumerThread.awaitReady(Duration.ofMillis(1)));
            // Third call too
            assertTrue(consumerThread.awaitReady(Duration.ZERO));
        } finally {
            consumerThread.shutdown();
            consumerThread.join(1000);
        }
    }
}
