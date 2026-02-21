package pl.perfluencer.kafka;

import io.gatling.commons.util.Clock;
import io.gatling.core.stats.StatsEngine;
import pl.perfluencer.cache.BatchProcessor;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.kafka.MessageProcessor.RetryEnvelope;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pekko.actor.ActorRef;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MessageProcessorRetryTest {

    @Mock
    private RequestStore requestStore;
    @Mock
    private StatsEngine statsEngine;
    @Mock
    private Clock clock;
    @Mock
    private ActorRef controller;
    @Mock
    private CorrelationExtractor correlationExtractor;

    private MessageProcessor messageProcessor;
    private final Duration retryBackoff = Duration.ofMillis(100);
    private final int maxRetries = 3;

    @Before
    public void setUp() {
        messageProcessor = new MessageProcessor(
                requestStore,
                statsEngine,
                clock,
                controller,
                Collections.emptyList(),
                null,
                correlationExtractor,
                false,
                retryBackoff,
                maxRetries);
    }

    @Test
    public void testRetryScheduledOnUnmatched() {
        // Given
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        RetryEnvelope envelope = new RetryEnvelope(record, 0); // 0 attempts
        List<RetryEnvelope> input = Collections.singletonList(envelope);

        when(correlationExtractor.extract(record)).thenReturn("correlationId");

        // When
        doAnswer(invocation -> {
            BatchProcessor processor = invocation.getArgument(1);
            // Simulate unmatched response
            processor.onUnmatched("correlationId", invocation.getArgument(0, Map.class).get("correlationId"));
            return null;
        }).when(requestStore).processBatchedRecords(any(), any());

        List<RetryEnvelope> result = messageProcessor.processRetries(input);

        // Then
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).attempts); // Attempts incremented
        assertEquals(record, result.get(0).record);
    }

    @Test
    public void testRetryExhaustion() {
        // Given
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        RetryEnvelope envelope = new RetryEnvelope(record, maxRetries); // Max attempts reached
        List<RetryEnvelope> input = Collections.singletonList(envelope);

        when(correlationExtractor.extract(record)).thenReturn("correlationId");

        // When
        doAnswer(invocation -> {
            BatchProcessor processor = invocation.getArgument(1);
            // Simulate unmatched response
            processor.onUnmatched("correlationId", invocation.getArgument(0, Map.class).get("correlationId"));
            return null;
        }).when(requestStore).processBatchedRecords(any(), any());

        List<RetryEnvelope> result = messageProcessor.processRetries(input);

        // Then
        assertTrue(result.isEmpty()); // No modification to retry list, means dropped from retry loop
        verify(statsEngine).logResponse(any(), any(), any(), anyLong(), anyLong(), any(), any(), any()); // Logged as
                                                                                                         // failure
    }

    @Test
    public void testSuccessfulMatchStopsRetry() {
        // Given
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        RetryEnvelope envelope = new RetryEnvelope(record, 1);
        List<RetryEnvelope> input = Collections.singletonList(envelope);

        when(correlationExtractor.extract(record)).thenReturn("correlationId");

        // When
        doAnswer(invocation -> {
            BatchProcessor processor = invocation.getArgument(1);
            // Simulate MATCHED response
            pl.perfluencer.cache.RequestData requestData = new pl.perfluencer.cache.RequestData(
                    "correlationId",
                    "key",
                    "value",
                    pl.perfluencer.common.util.SerializationType.STRING,
                    "txn",
                    "scn",
                    System.currentTimeMillis(),
                    30000,
                    null);
            processor.onMatch("correlationId", requestData, input);
            return null;
        }).when(requestStore).processBatchedRecords(any(), any());

        List<RetryEnvelope> result = messageProcessor.processRetries(input);

        // Then
        assertTrue(result.isEmpty()); // No retry needed
        verify(statsEngine).logResponse(any(), any(), any(), anyLong(), anyLong(), any(), any(), any()); // Logged as
                                                                                                         // success
    }

    @Test
    public void testGetRetryBackoff() {
        assertEquals(retryBackoff, messageProcessor.getRetryBackoff());
    }
}
