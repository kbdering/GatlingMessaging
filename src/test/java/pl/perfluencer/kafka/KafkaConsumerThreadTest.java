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

package pl.perfluencer.kafka;

import pl.perfluencer.kafka.consumers.KafkaConsumerThread;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KafkaConsumerThreadTest {

    @Test
    public void testPoll() throws Exception {
        String topic = "test-topic";
        MockConsumer<String, Object> mockConsumer = new MockConsumer<>(
                org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);
        TopicPartition tp = new TopicPartition(topic, 0);

        CountDownLatch recordsProcessed = new CountDownLatch(1);

        // Create a MessageProcessor that signals when records are processed
        MessageProcessor mockProcessor = mock(MessageProcessor.class);
        doAnswer(invocation -> {
            ConsumerRecords<?, ?> records = invocation.getArgument(0);
            if (!records.isEmpty()) {
                recordsProcessed.countDown();
            }
            return null;
        }).when(mockProcessor).process(any(ConsumerRecords.class));

        // Schedule the rebalance and record addition before starting consumer
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(Collections.singletonList(tp));
            mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 0, "key", "value"));
        });

        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                mockConsumer, topic, mockProcessor, null, Duration.ofMillis(100), false, 0);
        consumerThread.start();

        try {
            // Wait for records to be processed
            assertTrue("Records should have been processed",
                    recordsProcessed.await(5, TimeUnit.SECONDS));
            verify(mockProcessor, atLeastOnce()).process(any(ConsumerRecords.class));
        } finally {
            consumerThread.shutdown();
            consumerThread.join(1000);
        }
    }

    @Test
    public void testPollFailure() throws Exception {
        String topic = "test-topic";
        MockConsumer<String, Object> mockConsumer = new MockConsumer<>(
                org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);

        // Schedule an exception on poll
        mockConsumer.schedulePollTask(() -> {
            throw new org.apache.kafka.common.KafkaException("Poll failed");
        });

        MessageProcessor mockProcessor = mock(MessageProcessor.class);

        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                mockConsumer, topic, mockProcessor, null, Duration.ofMillis(100), false, 0);
        consumerThread.start();

        try {
            // Wait a bit for the error to occur and be handled
            Thread.sleep(500);

            // The thread should still be running (it handles errors gracefully)
            assertTrue("Consumer thread should still be running", consumerThread.isRunning());

            // Processor should not have been called with any records
            verify(mockProcessor, never()).process(any(ConsumerRecords.class));
        } finally {
            consumerThread.shutdown();
            consumerThread.join(1000);
        }
    }

    @Test
    public void testShutdown() throws Exception {
        String topic = "test-topic";
        MockConsumer<String, Object> mockConsumer = new MockConsumer<>(
                org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);
        TopicPartition tp = new TopicPartition(topic, 0);
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(Collections.singletonList(tp));
            mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
        });

        MessageProcessor mockProcessor = mock(MessageProcessor.class);

        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                mockConsumer, topic, mockProcessor, null, Duration.ofMillis(100), false, 0);
        consumerThread.start();

        Thread.sleep(200);
        assertTrue("Consumer should be running", consumerThread.isRunning());

        consumerThread.shutdown();
        consumerThread.join(2000);

        assertFalse("Consumer should have stopped", consumerThread.isRunning());
        assertFalse("Thread should have terminated", consumerThread.isAlive());
    }
}
