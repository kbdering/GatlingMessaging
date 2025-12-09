package pl.perfluencer.kafka.actors;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import pl.perfluencer.kafka.cache.BatchProcessor;

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