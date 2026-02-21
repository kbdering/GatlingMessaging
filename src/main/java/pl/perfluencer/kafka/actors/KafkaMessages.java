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

package pl.perfluencer.kafka.actors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import pl.perfluencer.cache.BatchProcessor;

public class KafkaMessages {
    public static final class Poll {
        public static final Poll INSTANCE = new Poll();

        private Poll() {
        }
    }

    // Message containing the record to be processed by a worker
    public static final class ProcessRecord { // Made non-generic for simplicity, assuming byte[]
        public final ConsumerRecord<String, byte[]> record; // Changed to byte[]
        public final long consumeEndTime;

        public ProcessRecord(ConsumerRecord<String, byte[]> record, long consumeEndTime) {
            this.record = record;
            this.consumeEndTime = consumeEndTime;
        }
    }
}