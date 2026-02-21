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
