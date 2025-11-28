package io.github.kbdering.kafka.extractors;

import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.nio.charset.StandardCharsets;

public class JsonPathExtractor implements CorrelationExtractor {
    private final String jsonPath;

    public JsonPathExtractor(String jsonPath) {
        this.jsonPath = jsonPath;
    }

    @Override
    public String extract(ConsumerRecord<String, Object> record) {
        if (record.value() == null)
            return null;

        String json;
        if (record.value() instanceof byte[]) {
            json = new String((byte[]) record.value(), StandardCharsets.UTF_8);
        } else if (record.value() instanceof String) {
            json = (String) record.value();
        } else {
            json = record.value().toString();
        }

        try {
            Object result = JsonPath.read(json, jsonPath);
            return result != null ? result.toString() : null;
        } catch (Exception e) {
            // Log or handle exception? For now return null to indicate extraction failure
            return null;
        }
    }
}
