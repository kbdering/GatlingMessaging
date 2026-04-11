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

package pl.perfluencer.kafka.javaapi;

import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;

import io.gatling.core.CoreComponents;
import io.gatling.core.config.GatlingConfiguration;
import io.gatling.core.protocol.Protocol;
import io.gatling.core.session.Session;
import io.gatling.javaapi.core.ProtocolBuilder;

import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.cache.RequestStore;
import pl.perfluencer.common.util.ParserRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.HashMap;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import io.gatling.core.protocol.ProtocolKey;
import scala.Function1;
import scala.runtime.BoxedUnit;

public final class KafkaProtocolBuilder implements ProtocolBuilder {

    private final Map<String, Object> producerProperties = new HashMap<>();
    private final Map<String, Object> consumerProperties = new HashMap<>();
    private String bootstrapServers = null;
    private String groupId = null;
    private ActorSystem actorSystem = null;
    private int numProducers = 1;
    private int numConsumers = 1;
    private RequestStore requestStore;
    private String correlationHeaderName = "correlationId";
    private CorrelationExtractor correlationExtractor = new pl.perfluencer.kafka.extractors.KeyExtractor();

    private Duration pollTimeout = Duration.ofMillis(100);
    private pl.perfluencer.cache.config.CoreStoreConfig storeConfig = new pl.perfluencer.cache.config.CoreStoreConfig();
    private boolean syncCommit = false;
    private boolean awaitConsumersReady = false;
    private boolean seekToEndOnReady = false;
    private Duration consumerReadyTimeout = Duration.ofSeconds(30);

    private final java.util.List<pl.perfluencer.kafka.MessageCheck<?, ?>> globalChecks = new java.util.ArrayList<>();
    private final ParserRegistry parserRegistry = new ParserRegistry();

    public KafkaProtocolBuilder check(pl.perfluencer.kafka.MessageCheck<?, ?> check) {
        this.globalChecks.add(check);
        return this;
    }

    public KafkaProtocolBuilder checks(pl.perfluencer.kafka.MessageCheck<?, ?>... checks) {
        if (checks != null) {
            java.util.Collections.addAll(this.globalChecks, checks);
        }
        return this;
    }

    /**
     * Convenience method to add a check directly from a builder result (e.g.
     * jsonPathEquals).
     * Automatically wraps it in MessageCheck.from().
     */
    public KafkaProtocolBuilder check(
            pl.perfluencer.common.checks.MessageCheckBuilder.MessageCheckResult<?, ?> result) {
        // Default to STRING serialization for global checks usually involving text
        // (JSON/XML)
        // If specific types are needed, user should use the manual MessageCheck.from()
        this.globalChecks.add(pl.perfluencer.kafka.MessageCheck.from(result));
        return this;
    }

    public KafkaProtocolBuilder storeConfig(
            java.util.function.Consumer<pl.perfluencer.cache.config.CoreStoreConfig> configurer) {
        configurer.accept(this.storeConfig);
        return this;
    }

    /**
     * Registers a parser for a Protobuf or Avro class to avoid reflection overhead.
     * 
     * <p>
     * Example:
     * 
     * <pre>{@code
     * protocol.registerParser(OrderRequest.class, OrderRequest::parseFrom);
     * }</pre>
     * 
     * @param clazz  the class type to register
     * @param parser function that converts byte[] to the target type
     * @param <T>    the target type
     * @return this builder for chaining
     */
    public <T> KafkaProtocolBuilder registerParser(Class<T> clazz, java.util.function.Function<byte[], T> parser) {
        parserRegistry.register(clazz, parser);
        return this;
    }

    /**
     * Returns the parser registry for passing to MessageProcessor.
     */
    public ParserRegistry getParserRegistry() {
        return parserRegistry;
    }

    public KafkaProtocolBuilder pollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    /**
     * Enables synchronous offset commit mode. When true, offsets are committed
     * synchronously after each batch, providing stronger delivery guarantees.
     * When false (default), offsets are committed asynchronously for better
     * performance.
     */
    public KafkaProtocolBuilder syncCommit(boolean syncCommit) {
        this.syncCommit = syncCommit;
        return this;
    }

    /**
     * When enabled, the action builders will block until all consumer threads have
     * been assigned partitions by the Kafka group coordinator before allowing any
     * messages to be sent. This prevents a race condition where messages are produced
     * before consumers are ready to receive them.
     * Default: {@code false}.
     */
    public KafkaProtocolBuilder awaitConsumersReady(boolean awaitConsumersReady) {
        this.awaitConsumersReady = awaitConsumersReady;
        return this;
    }

    /**
     * When enabled, each consumer thread will forcibly seek to the end of all
     * assigned partitions once it connects. This guarantees that only messages
     * produced <em>after</em> the consumer is ready will be consumed, skipping
     * any pre-existing messages on the topic (e.g., from a previous test run).
     * Default: {@code false}.
     */
    public KafkaProtocolBuilder seekToEndOnReady(boolean seekToEndOnReady) {
        this.seekToEndOnReady = seekToEndOnReady;
        return this;
    }

    /**
     * Maximum time to wait for all consumer threads to become ready (receive
     * partition assignment from the Kafka group coordinator). If the timeout
     * expires before all consumers are ready, an {@code IllegalStateException}
     * is thrown and the simulation fails fast.
     * Default: 30 seconds.
     */
    public KafkaProtocolBuilder consumerReadyTimeout(Duration timeout) {
        this.consumerReadyTimeout = timeout;
        return this;
    }

    private Duration retryBackoff = Duration.ofMillis(50);
    private int maxRetries = 3;

    /**
     * Delay between retry attempts when a matching request is not found in the
     * store.
     * Useful for eventual consistency when using request stores.
     * Default: 50ms.
     */
    public KafkaProtocolBuilder retryBackoff(Duration retryBackoff) {
        this.retryBackoff = retryBackoff;
        return this;
    }

    /**
     * Maximum number of times to retry looking up a request in the store before
     * marking it as failed.
     * Default: 3.
     */
    public KafkaProtocolBuilder maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    private Duration timeoutCheckInterval = Duration.ofSeconds(5);

    public KafkaProtocolBuilder timeoutCheckInterval(Duration interval) {
        this.timeoutCheckInterval = interval;
        this.storeConfig.timeoutCheckInterval(interval);
        return this;
    }

    public KafkaProtocolBuilder correlationExtractor(CorrelationExtractor extractor) {
        this.correlationExtractor = extractor;
        return this;
    }

    public KafkaProtocolBuilder correlationByKey() {
        this.correlationExtractor = new pl.perfluencer.kafka.extractors.KeyExtractor();
        return this;
    }

    public KafkaProtocolBuilder correlationHeaderName(String name) {
        this.correlationHeaderName = name;
        this.correlationExtractor = new pl.perfluencer.kafka.extractors.HeaderExtractor(name);
        return this;
    }

    public KafkaProtocolBuilder bootstrapServers(String servers) {
        this.bootstrapServers = servers;
        return this;
    }

    public KafkaProtocolBuilder actorSystem(ActorSystem system) {
        this.actorSystem = system;
        return this;
    }

    public KafkaProtocolBuilder numProducers(int numProducers) {
        this.numProducers = numProducers;
        return this;
    }

    public KafkaProtocolBuilder numConsumers(int numConsumers) {
        this.numConsumers = numConsumers;
        return this;
    }

    public KafkaProtocolBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public KafkaProtocolBuilder producerProperties(Map<String, Object> props) {
        producerProperties.putAll(props);
        return this;
    }

    public KafkaProtocolBuilder producer(org.apache.kafka.clients.producer.MockProducer<String, Object> mockProducer) {
        producerProperties.put("mockProducer", mockProducer);
        return this;
    }

    public KafkaProtocolBuilder consumerProperties(Map<String, Object> props) {
        consumerProperties.putAll(props);
        return this;
    }

    public KafkaProtocolBuilder requestStore(RequestStore requestStore) {
        this.requestStore = requestStore;
        return this;
    }

    private Duration metricInjectionInterval = null;

    public KafkaProtocolBuilder metricInjectionInterval(Duration interval) {
        this.metricInjectionInterval = interval;
        return this;
    }

    private boolean useTimestampHeader = false;

    public KafkaProtocolBuilder useTimestampHeader(boolean useTimestampHeader) {
        this.useTimestampHeader = useTimestampHeader;
        return this;
    }

    private String transactionalId = null;

    public KafkaProtocolBuilder transactionalId(String transactionalId) {
        this.transactionalId = transactionalId;
        return this;
    }

    private boolean measureStoreLatency = false;

    public KafkaProtocolBuilder measureStoreLatency(boolean measureStoreLatency) {
        this.measureStoreLatency = measureStoreLatency;
        return this;
    }

    @Override
    public Protocol protocol() {
        return build();
    }

    public static class ConsumerAndProcessor {
        public final java.util.List<pl.perfluencer.kafka.consumers.KafkaConsumerThread> consumerThreads;
        public final pl.perfluencer.kafka.MessageProcessor messageProcessor;

        public ConsumerAndProcessor(java.util.List<pl.perfluencer.kafka.consumers.KafkaConsumerThread> consumerThreads,
                pl.perfluencer.kafka.MessageProcessor messageProcessor) {
            this.consumerThreads = consumerThreads;
            this.messageProcessor = messageProcessor;
        }

        public void shutdown() {
            for (pl.perfluencer.kafka.consumers.KafkaConsumerThread thread : consumerThreads) {
                thread.shutdown();
            }
        }

        /**
         * Blocks until all consumer threads have been assigned partitions.
         *
         * @param timeout maximum time to wait for each thread
         * @return {@code true} if all threads became ready, {@code false} if any timed out
         */
        public boolean awaitAllReady(Duration timeout) {
            for (pl.perfluencer.kafka.consumers.KafkaConsumerThread thread : consumerThreads) {
                if (!thread.awaitReady(timeout)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class KafkaProtocol implements Protocol {
        private final Map<String, Object> producerProperties;
        private final Map<String, Object> consumerProperties;
        private final ActorSystem actorSystem;
        private final int numProducers;
        private final int numConsumers;
        private final RequestStore requestStore;
        private final CorrelationExtractor correlationExtractor;
        private final Duration pollTimeout;
        private final Duration metricInjectionInterval;
        private final String correlationHeaderName;
        private final boolean useTimestampHeader;
        private final String transactionalId;
        private final boolean measureStoreLatency;
        private final Duration retryBackoff;
        private final int maxRetries;
        private final boolean syncCommit;
        private final boolean awaitConsumersReady;
        private final boolean seekToEndOnReady;
        private final Duration consumerReadyTimeout;

        private final List<KafkaProducer<String, Object>> producers = new ArrayList<>();
        private final AtomicInteger producerIndex = new AtomicInteger(0);
        private final Map<String, ConsumerAndProcessor> consumerAndProcessorsByTopic = new ConcurrentHashMap<>();
        private final java.util.concurrent.ConcurrentMap<String, java.util.List<pl.perfluencer.kafka.MessageCheck<?, ?>>> checkRegistry = new ConcurrentHashMap<>();
        private final java.util.List<pl.perfluencer.kafka.MessageCheck<?, ?>> globalChecks;
        private final ParserRegistry parserRegistry;

        private KafkaProtocol(Map<String, Object> producerProperties, Map<String, Object> consumerProperties,
                ActorSystem actorSystem, int numProducers, int numConsumers, RequestStore requestStore,
                CorrelationExtractor correlationExtractor, Duration pollTimeout, Duration metricInjectionInterval,
                String correlationHeaderName, boolean useTimestampHeader, String transactionalId,
                boolean measureStoreLatency,
                Duration retryBackoff, int maxRetries, boolean syncCommit,
                boolean awaitConsumersReady, boolean seekToEndOnReady, Duration consumerReadyTimeout,
                java.util.List<pl.perfluencer.kafka.MessageCheck<?, ?>> globalChecks,
                ParserRegistry parserRegistry) {
            this.producerProperties = producerProperties;
            this.consumerProperties = consumerProperties;
            this.actorSystem = actorSystem;
            this.numProducers = numProducers;
            this.numConsumers = numConsumers;
            this.requestStore = requestStore;
            this.correlationExtractor = correlationExtractor;
            this.pollTimeout = pollTimeout;
            this.metricInjectionInterval = metricInjectionInterval;
            this.correlationHeaderName = correlationHeaderName;
            this.useTimestampHeader = useTimestampHeader;
            this.transactionalId = transactionalId;
            this.measureStoreLatency = measureStoreLatency;
            this.retryBackoff = retryBackoff;
            this.maxRetries = maxRetries;
            this.syncCommit = syncCommit;
            this.awaitConsumersReady = awaitConsumersReady;
            this.seekToEndOnReady = seekToEndOnReady;
            this.consumerReadyTimeout = consumerReadyTimeout;
            this.globalChecks = globalChecks != null ? new java.util.ArrayList<>(globalChecks)
                    : java.util.Collections.emptyList();
            this.parserRegistry = parserRegistry;
        }

        public java.util.List<pl.perfluencer.kafka.MessageCheck<?, ?>> getGlobalChecks() {
            return globalChecks;
        }

        public boolean isMeasureStoreLatency() {
            return measureStoreLatency;
        }

        public Map<String, Object> getProducerProperties() {
            return producerProperties;
        }

        public Map<String, Object> getConsumerProperties() {
            return consumerProperties;
        }

        public ActorSystem getActorSystem() {
            return actorSystem;
        }

        public int getNumProducers() {
            return numProducers;
        }

        public int getNumConsumers() {
            return numConsumers;
        }

        public RequestStore getRequestStore() {
            return requestStore;
        }

        public CorrelationExtractor getCorrelationExtractor() {
            return correlationExtractor;
        }

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public Duration getMetricInjectionInterval() {
            return metricInjectionInterval;
        }

        public String getCorrelationHeaderName() {
            return correlationHeaderName;
        }

        public boolean isUseTimestampHeader() {
            return useTimestampHeader;
        }

        public Duration getRetryBackoff() {
            return retryBackoff;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public boolean isSyncCommit() {
            return syncCommit;
        }

        public boolean isAwaitConsumersReady() {
            return awaitConsumersReady;
        }

        public boolean isSeekToEndOnReady() {
            return seekToEndOnReady;
        }

        public Duration getConsumerReadyTimeout() {
            return consumerReadyTimeout;
        }

        public String getTransactionalId() {
            return transactionalId;
        }

        public KafkaProducer<String, Object> getProducer() {
            if (producers.isEmpty()) {
                return null;
            }
            int index = producerIndex.getAndIncrement() % producers.size();
            return producers.get(Math.abs(index));
        }

        public List<KafkaProducer<String, Object>> getProducers() {
            return producers;
        }

        public void setProducers(List<KafkaProducer<String, Object>> producers) {
            this.producers.clear();
            this.producers.addAll(producers);
        }

        public ConsumerAndProcessor getConsumerAndProcessor(String topic) {
            return consumerAndProcessorsByTopic.get(topic);
        }

        public void putConsumerAndProcessor(String topic, ConsumerAndProcessor consumerAndProcessor) {
            this.consumerAndProcessorsByTopic.put(topic, consumerAndProcessor);
        }

        private final Map<String, ActorRef> rawConsumerActorsByTopic = new ConcurrentHashMap<>();

        public ActorRef getRawConsumerActor(String topic) {
            return rawConsumerActorsByTopic.computeIfAbsent(topic, t -> {
                return actorSystem.actorOf(pl.perfluencer.kafka.actors.KafkaRawConsumerActor
                        .props(consumerProperties, t, pollTimeout));
            });
        }

        public java.util.List<pl.perfluencer.kafka.MessageCheck<?, ?>> getChecks(String requestName) {
            return checkRegistry.getOrDefault(requestName, java.util.Collections.emptyList());
        }

        public void registerChecks(String requestName, java.util.List<pl.perfluencer.kafka.MessageCheck<?, ?>> checks) {
            if (checks != null && !checks.isEmpty()) {
                checkRegistry.put(requestName, checks);
            }
        }

        public java.util.concurrent.ConcurrentMap<String, java.util.List<pl.perfluencer.kafka.MessageCheck<?, ?>>> getCheckRegistry() {
            return checkRegistry;
        }

        public ParserRegistry getParserRegistry() {
            return parserRegistry;
        }
    }

    public static final class KafkaProtocolComponents implements io.gatling.core.protocol.ProtocolComponents {
        private final KafkaProtocol kafkaProtocol;
        private final CoreComponents coreComponents;
        private java.util.concurrent.ScheduledExecutorService metricScheduler;

        public KafkaProtocolComponents(KafkaProtocol kafkaProtocol, CoreComponents coreComponents) {
            this.kafkaProtocol = kafkaProtocol;
            this.coreComponents = coreComponents;

            if (kafkaProtocol.getProducers().isEmpty()) {
                // Initialize KafkaProducer pool
                List<KafkaProducer<String, Object>> producers = new ArrayList<>();
                int numProducers = kafkaProtocol.getNumProducers();
                for (int i = 0; i < numProducers; i++) {
                    producers.add(new KafkaProducer<>(kafkaProtocol.getProducerProperties()));
                }
                kafkaProtocol.setProducers(producers);
                System.out.println("DEBUG: KafkaProtocolComponents instantiated - Created pool of " + numProducers
                        + " KafkaProducers");
            }
        }

        public Function1<Session, Session> onStart() {
            kafkaProtocol.getRequestStore().startTimeoutMonitoring(new pl.perfluencer.cache.TimeoutHandler() {
                @Override
                public void onTimeout(String correlationId, pl.perfluencer.cache.RequestData requestData) {
                    long startTime = requestData.startTime;
                    long endTime = System.currentTimeMillis();
                    String transactionName = requestData.transactionName;
                    String scenarioName = requestData.scenarioName;

                    coreComponents.statsEngine().logResponse(
                            scenarioName,
                            scala.collection.immutable.List$.MODULE$.from(scala.jdk.javaapi.CollectionConverters
                                    .asScala(java.util.Collections.singletonList("End-to-End Group"))),
                            transactionName,
                            startTime,
                            endTime,
                            io.gatling.commons.stats.Status.apply("KO"),
                            scala.Option.apply("504"),
                            scala.Option.apply("Request timed out"));
                }
            });

            if (kafkaProtocol.getMetricInjectionInterval() != null) {
                startMetricInjection();
            }

            return session -> session;
        }

        private void startMetricInjection() {
            LoggerFactory.getLogger(KafkaProtocolBuilder.class).info(
                    "Starting Kafka Metric Injection scheduler with interval: {}",
                    kafkaProtocol.getMetricInjectionInterval());
            metricScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "KafkaMetricInjector");
                t.setDaemon(true);
                return t;
            });

            metricScheduler.scheduleAtFixedRate(() -> {
                try {
                    javax.management.MBeanServer mBeanServer = java.lang.management.ManagementFactory
                            .getPlatformMBeanServer();
                    long now = System.currentTimeMillis();

                    // Consumer Lag Max
                    java.util.Set<javax.management.ObjectName> consumerMetrics = mBeanServer.queryNames(
                            new javax.management.ObjectName("kafka.consumer:type=consumer-metrics,client-id=*"), null);

                    double maxLag = 0.0;
                    for (javax.management.ObjectName name : consumerMetrics) {
                        try {
                            Object val = mBeanServer.getAttribute(name, "records-lag-max");
                            if (val instanceof Number) {
                                maxLag = Math.max(maxLag, ((Number) val).doubleValue());
                            }
                        } catch (Exception ignored) {
                        }
                    }

                    // Log Lag as a "Response Time" (so we can assert on max value)
                    // We log it as OK request
                    long duration = (long) maxLag;
                    if (duration <= 0)
                        duration = 1; // ensure positive duration for Gatling StatsEngine

                    LoggerFactory.getLogger(KafkaProtocolBuilder.class)
                            .debug("Injecting Kafka Metric kafka-consumer-lag-max: {}", maxLag);
                    coreComponents.statsEngine().logResponse(
                            "Kafka Metrics",
                            scala.collection.immutable.List$.MODULE$.empty(),
                            "kafka-consumer-lag-max",
                            now - duration, // Start time = now - duration, so Gatling logs duration
                            now,
                            io.gatling.commons.stats.Status.apply("OK"),
                            scala.Option.empty(),
                            scala.Option.empty());

                } catch (Exception e) {
                    LoggerFactory.getLogger(KafkaProtocolBuilder.class).error("Error in Kafka Metric Injector", e);
                }
            }, 2000, kafkaProtocol.getMetricInjectionInterval().toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        public Function1<Session, BoxedUnit> onExit() {
            return session -> {
                kafkaProtocol.getRequestStore().stopTimeoutMonitoring();
                if (metricScheduler != null) {
                    metricScheduler.shutdownNow();
                }
                // Do NOT close producers here. onExit is called for each user session
                // termination.
                // Closing shared producers here would break the simulation for other running
                // users.
                // Producers will be closed when the JVM terminates or we can implement a proper
                // shutdown hook if needed.
                return BoxedUnit.UNIT;
            };
        }

        public Protocol protocol() {
            return kafkaProtocol;
        }

        public static final ProtocolKey<KafkaProtocol, KafkaProtocolComponents> protocolKey = new ProtocolKey<>() {
            public Class<Protocol> protocolClass() {
                return (Class<Protocol>) (Class<?>) KafkaProtocol.class;
            }

            public KafkaProtocol defaultProtocolValue(GatlingConfiguration configuration) {
                return new KafkaProtocolBuilder().bootstrapServers("localhost:9092").groupId("default-gatling-group")
                        .build();
            }

            public Function1<KafkaProtocol, KafkaProtocolComponents> newComponents(CoreComponents coreComponents) {
                return kafkaProtocol -> new KafkaProtocolComponents(kafkaProtocol, coreComponents);
            }
        };
    }

    public KafkaProtocol build() {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Kafka bootstrap servers are not configured.\n" +
                            "Potential Fix: Ensure you call .bootstrapServers(\"host:port\") in your KafkaProtocolBuilder.");
        }

        // Only require groupId if we have consumers
        if (numConsumers > 0 && (groupId == null || groupId.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "Kafka consumer group ID is not configured, but numConsumers is > 0.\n" +
                            "Potential Fix: Ensure you call .groupId(\"my-group-id\") in your KafkaProtocolBuilder.");
        }

        if (numProducers <= 0) {
            throw new IllegalArgumentException(
                    "numProducers must be greater than 0.\n" +
                            "Potential Fix: Check your .numProducers() configuration.");
        }

        if (numConsumers < 0) {
            throw new IllegalArgumentException(
                    "numConsumers must be non-negative.\n" +
                            "Potential Fix: Check your .numConsumers() configuration.");
        }

        if (actorSystem == null) {
            actorSystem = ActorSystem.create("GatlingKafkaSystem");
        }

        if (requestStore == null) {
            requestStore = new InMemoryRequestStore(storeConfig);
        }

        producerProperties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());

        consumerProperties.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerProperties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        if (groupId != null) {
            consumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        consumerProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Safer defaults to prevent OOM/blocking when broker is down
        producerProperties.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000); // 10 seconds
        producerProperties.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000); // 30 seconds

        return new KafkaProtocol(
                new HashMap<>(producerProperties),
                new HashMap<>(consumerProperties),
                actorSystem,
                numProducers,
                numConsumers,
                requestStore,
                correlationExtractor,
                pollTimeout,
                metricInjectionInterval,
                correlationHeaderName,
                useTimestampHeader,
                transactionalId,
                measureStoreLatency,
                retryBackoff,
                maxRetries,
                syncCommit,
                awaitConsumersReady,
                seekToEndOnReady,
                consumerReadyTimeout,
                globalChecks,
                parserRegistry);
    }
}
