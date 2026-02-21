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
import java.util.function.Function;

public class FunctionExtractor implements CorrelationExtractor {
    private final Function<ConsumerRecord<String, Object>, String> extractorFunction;

    public FunctionExtractor(Function<ConsumerRecord<String, Object>, String> extractorFunction) {
        this.extractorFunction = extractorFunction;
    }

    @Override
    public String extract(ConsumerRecord<String, Object> record) {
        return extractorFunction.apply(record);
    }
}
