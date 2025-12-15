package pl.perfluencer.kafka.extractors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.nio.charset.StandardCharsets;

/**
 * Extracts the correlation ID from the Kafka message key.
 */
public class KeyExtractor implements CorrelationExtractor {

    @Override
    public String extract(ConsumerRecord<String, Object> record) {
        if (record.key() == null) {
            return null;
        }
        return record.key();
    }
}
