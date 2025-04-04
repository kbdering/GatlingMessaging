package io.github.kbdering.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaMessages {
    public static final class Poll {
        public static final Poll INSTANCE = new Poll();
        private Poll() {}
    }

    // Message containing the record to be processed by a worker
    public static final class ProcessRecord<T> {
        public final ConsumerRecord<String, T> record;
        public final long consumeEndTime;

        public ProcessRecord(ConsumerRecord<String, T> record, long consumeEndTime) {
            this.record = record;
            this.consumeEndTime = consumeEndTime;
        }
    }
}