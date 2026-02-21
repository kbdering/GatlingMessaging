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

package pl.perfluencer.kafka.actions;

import io.gatling.core.action.Action;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.core.structure.ScenarioContext;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.common.util.SerializationType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Action builder for consume-only mode.
 *
 * <p>
 * Sets up {@code KafkaConsumerThread} instances in consume-only mode,
 * where records are processed without request correlation and response-only
 * checks are applied to every consumed record.
 * </p>
 *
 * <p>
 * This builder does NOT create a sending action. The Gatling action itself
 * is a no-op pass-through; the actual work is done by the consumer threads
 * running in the background.
 * </p>
 * 
 * @param <T> The expected class type of the consumed messages
 */
public class KafkaConsumeActionBuilder<T> implements ActionBuilder {

    private static final scala.collection.immutable.List<String> CONSUME_ONLY_GROUP = scala.collection.immutable.List$.MODULE$
            .from(
                    scala.jdk.javaapi.CollectionConverters
                            .asScala(java.util.Collections.singletonList("Consume Only Group")));

    private final String requestName;
    private final String topic;
    private final List<MessageCheck<?, ?>> checks;
    private final Class<T> expectedClass;
    private final SerializationType expectedSerde;

    @SuppressWarnings("unchecked")
    public KafkaConsumeActionBuilder(String requestName, String topic) {
        this(requestName, topic, new ArrayList<>(), (Class<T>) byte[].class, SerializationType.BYTE_ARRAY);
    }

    private KafkaConsumeActionBuilder(String requestName, String topic, List<MessageCheck<?, ?>> checks,
            Class<T> expectedClass, SerializationType expectedSerde) {
        this.requestName = requestName;
        this.topic = topic;
        this.checks = new ArrayList<>(checks);
        this.expectedClass = expectedClass;
        this.expectedSerde = expectedSerde;
    }

    // ==================== SERIALIZATION HINTS ====================

    public <U> KafkaConsumeActionBuilder<U> asAvro(Class<U> clazz) {
        return new KafkaConsumeActionBuilder<>(requestName, topic, checks, clazz, SerializationType.AVRO);
    }

    public <U> KafkaConsumeActionBuilder<U> asProtobuf(Class<U> clazz) {
        return new KafkaConsumeActionBuilder<>(requestName, topic, checks, clazz, SerializationType.PROTOBUF);
    }

    public KafkaConsumeActionBuilder<String> asString() {
        return new KafkaConsumeActionBuilder<>(requestName, topic, checks, String.class, SerializationType.STRING);
    }

    public KafkaConsumeActionBuilder<byte[]> asBytes() {
        return new KafkaConsumeActionBuilder<>(requestName, topic, checks, byte[].class, SerializationType.BYTE_ARRAY);
    }

    // ==================== CHECKS ====================

    /**
     * Registers a simplified check that only validates the consumed message.
     * The type is inferred from previous serialization hint methods (e.g.
     * asAvro(MyClass.class)).
     */
    public KafkaConsumeActionBuilder<T> check(String checkName, Function<T, Optional<String>> checkLogic) {
        MessageCheck<Object, T> check = new MessageCheck<>(
                checkName,
                Object.class, SerializationType.STRING, // Ignored in consume-only
                expectedClass, expectedSerde,
                (req, res) -> checkLogic.apply(res));
        this.checks.add(check);
        return this;
    }

    public KafkaConsumeActionBuilder<T> check(MessageCheck<?, ?> check) {
        this.checks.add(check);
        return this;
    }

    @SuppressWarnings("unchecked")
    public KafkaConsumeActionBuilder<T> check(
            pl.perfluencer.common.checks.MessageCheckBuilder.MessageCheckResult<?, T> check) {
        this.checks.add(MessageCheck.from(
                (pl.perfluencer.common.checks.MessageCheckBuilder.MessageCheckResult<Object, T>) check,
                SerializationType.STRING, this.expectedSerde));
        return this;
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() {
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                KafkaProtocolBuilder.KafkaProtocolComponents components = ctx.protocolComponentsRegistry()
                        .components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey);
                KafkaProtocolBuilder.KafkaProtocol concreteProtocol = (KafkaProtocolBuilder.KafkaProtocol) components
                        .protocol();

                // Register checks for this request name
                if (!checks.isEmpty()) {
                    concreteProtocol.registerChecks(requestName, checks);
                }

                // Only create consumer threads once per topic
                if (concreteProtocol.getConsumerAndProcessor(topic) == null) {
                    // Create MessageProcessor (shared across all consumer threads)
                    pl.perfluencer.kafka.MessageProcessor messageProcessor = new pl.perfluencer.kafka.MessageProcessor(
                            concreteProtocol.getRequestStore(),
                            ctx.coreComponents().statsEngine(),
                            ctx.coreComponents().clock(),
                            null, // No actor controller needed
                            concreteProtocol.getCheckRegistry(),
                            concreteProtocol.getGlobalChecks(),
                            null, // Session-aware checks not used
                            concreteProtocol.getCorrelationExtractor(),
                            concreteProtocol.isUseTimestampHeader(),
                            concreteProtocol.getRetryBackoff(),
                            concreteProtocol.getMaxRetries(),
                            concreteProtocol.getParserRegistry(),
                            CONSUME_ONLY_GROUP);

                    // Create consumer threads in consume-only mode
                    java.util.List<pl.perfluencer.kafka.consumers.KafkaConsumerThread> consumerThreads = new java.util.ArrayList<>();
                    String scenarioName = "ConsumeOnly-" + requestName;
                    for (int i = 0; i < concreteProtocol.getNumConsumers(); i++) {
                        pl.perfluencer.kafka.consumers.KafkaConsumerThread thread = new pl.perfluencer.kafka.consumers.KafkaConsumerThread(
                                concreteProtocol.getConsumerProperties(),
                                topic,
                                messageProcessor,
                                ctx.coreComponents().statsEngine(),
                                concreteProtocol.getPollTimeout(),
                                concreteProtocol.isSyncCommit(),
                                i,
                                requestName, // consume-only request name
                                scenarioName, // consume-only scenario name
                                CONSUME_ONLY_GROUP);
                        thread.start();
                        consumerThreads.add(thread);
                    }

                    concreteProtocol.putConsumerAndProcessor(topic,
                            new KafkaProtocolBuilder.ConsumerAndProcessor(consumerThreads, messageProcessor));
                }

                // The action itself is a pass-through — consumers do the work in background
                return new KafkaConsumeAction(requestName, ctx.coreComponents(), next);
            }
        };
    }
}
