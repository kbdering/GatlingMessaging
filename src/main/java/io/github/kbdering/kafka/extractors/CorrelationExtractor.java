package io.github.kbdering.kafka.extractors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface CorrelationExtractor {
    String extract(ConsumerRecord<String, Object> record);
}
