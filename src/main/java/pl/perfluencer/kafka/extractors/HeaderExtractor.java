package pl.perfluencer.kafka.extractors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import java.nio.charset.StandardCharsets;

/**
 * Extracts the correlation ID from a specific Kafka message header.
 */
public class HeaderExtractor implements CorrelationExtractor {

    private final String headerName;

    public HeaderExtractor(String headerName) {
        if (headerName == null || headerName.trim().isEmpty()) {
            throw new IllegalArgumentException("Header name cannot be null or empty");
        }
        this.headerName = headerName;
    }

    @Override
    public String extract(ConsumerRecord<String, Object> record) {
        if (record.headers() == null) {
            return null;
        }
        // Optimized lookup using lastHeader which avoids creating an iterator
        Header header = record.headers().lastHeader(headerName);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }
}
