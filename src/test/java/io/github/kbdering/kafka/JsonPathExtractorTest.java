package io.github.kbdering.kafka;

import io.github.kbdering.kafka.extractors.JsonPathExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import java.nio.charset.StandardCharsets;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JsonPathExtractorTest {

    @Test
    public void testExtractCorrelationId() {
        String json = "{\"correlationId\": \"12345\", \"data\": \"test\"}";
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key",
                (Object) json.getBytes(StandardCharsets.UTF_8));

        JsonPathExtractor extractor = new JsonPathExtractor("$.correlationId");
        String extracted = extractor.extract(record);

        assertEquals("12345", extracted);
    }

    @Test
    public void testExtractNestedCorrelationId() {
        String json = "{\"meta\": {\"id\": \"abc-def\"}, \"data\": \"test\"}";
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key",
                (Object) json.getBytes(StandardCharsets.UTF_8));

        JsonPathExtractor extractor = new JsonPathExtractor("$.meta.id");
        String extracted = extractor.extract(record);

        assertEquals("abc-def", extracted);
    }

    @Test
    public void testExtractMissingPath() {
        String json = "{\"data\": \"test\"}";
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0, "key",
                (Object) json.getBytes(StandardCharsets.UTF_8));

        JsonPathExtractor extractor = new JsonPathExtractor("$.correlationId");
        String extracted = extractor.extract(record);

        assertNull(extracted);
    }
}
