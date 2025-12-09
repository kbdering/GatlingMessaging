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
