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

import pl.perfluencer.kafka.extractors.JsonPathExtractor;
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
