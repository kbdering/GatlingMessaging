
package pl.perfluencer.kafka.javaapi;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.routing.RoundRobinPool;
import io.gatling.core.CoreComponents;
import io.gatling.core.config.GatlingConfiguration;
import io.gatling.core.protocol.Protocol;
import io.gatling.core.session.Session;
import io.gatling.javaapi.core.ProtocolBuilder;
import pl.perfluencer.kafka.actors.KafkaConsumerActor;
import pl.perfluencer.kafka.actors.KafkaMessages;
import pl.perfluencer.kafka.actors.KafkaProducerActor;
import pl.perfluencer.kafka.MessageCheck;
import pl.perfluencer.kafka.actors.MessageProcessorActor;
import pl.perfluencer.kafka.extractors.CorrelationExtractor;
import pl.perfluencer.kafka.cache.InMemoryRequestStore;
import pl.perfluencer.kafka.cache.RequestStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public KafkaProtocolBuilder pollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    private Duration timeoutCheckInterval = Duration.ofSeconds(5);

    public KafkaProtocolBuilder timeoutCheckInterval(Duration interval) {
        this.timeoutCheckInterval = interval;
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
        public final ActorRef consumerRouter;
        public final ActorRef messageProcessorRouter;

        public ConsumerAndProcessor(ActorRef consumerRouter, ActorRef messageProcessorRouter) {
            this.consumerRouter = consumerRouter;
            this.messageProcessorRouter = messageProcessorRouter;
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
        private ActorRef producerRouter;
        private final Map<String, ConsumerAndProcessor> consumerAndProcessorsByTopic = new ConcurrentHashMap<>();

        private KafkaProtocol(Map<String, Object> producerProperties, Map<String, Object> consumerProperties,
                ActorSystem actorSystem, int numProducers, int numConsumers, RequestStore requestStore,
                CorrelationExtractor correlationExtractor, Duration pollTimeout, Duration metricInjectionInterval,
                String correlationHeaderName, boolean useTimestampHeader, String transactionalId,
                boolean measureStoreLatency) {
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

        public String getTransactionalId() {
            return transactionalId;
        }

        public ActorRef getProducerRouter() {
            return producerRouter;
        }

        public void setProducerRouter(ActorRef producerRouter) {
            this.producerRouter = producerRouter;
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
    }

    public static final class KafkaProtocolComponents implements io.gatling.core.protocol.ProtocolComponents {
        private final KafkaProtocol kafkaProtocol;
        private final CoreComponents coreComponents;
        private java.util.concurrent.ScheduledExecutorService metricScheduler;

        public KafkaProtocolComponents(KafkaProtocol kafkaProtocol, CoreComponents coreComponents) {
            this.kafkaProtocol = kafkaProtocol;
            this.coreComponents = coreComponents;
            String componentId = coreComponents != null ? coreComponents.toString() : "test";
            if (kafkaProtocol.getProducerRouter() == null) {
                if (kafkaProtocol.getTransactionalId() != null) {
                    // Transactional producers must have unique IDs
                    java.util.List<String> routeePaths = new java.util.ArrayList<>();
                    for (int i = 0; i < kafkaProtocol.getNumProducers(); i++) {
                        Map<String, Object> props = new HashMap<>(kafkaProtocol.getProducerProperties());
                        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, kafkaProtocol.getTransactionalId() + "-" + i);
                        ActorRef producer = kafkaProtocol.getActorSystem().actorOf(
                                KafkaProducerActor.props(props, coreComponents.statsEngine(), coreComponents.clock()),
                                "kafkaProducer-" + componentId + "-" + i);
                        routeePaths.add(producer.path().toStringWithoutAddress());
                    }
                    ActorRef producerRouter = kafkaProtocol.getActorSystem().actorOf(
                            new org.apache.pekko.routing.RoundRobinGroup(routeePaths).props(),
                            "kafkaProducerRouter-" + componentId);
                    kafkaProtocol.setProducerRouter(producerRouter);
                } else {
                    ActorRef producerRouter = kafkaProtocol.getActorSystem().actorOf(
                            new RoundRobinPool(kafkaProtocol.getNumProducers())
                                    .props(KafkaProducerActor.props(kafkaProtocol.getProducerProperties(),
                                            coreComponents.statsEngine(), coreComponents.clock())),
                            "kafkaProducerRouter-" + componentId);
                    kafkaProtocol.setProducerRouter(producerRouter);
                }
            }
            System.out.println("DEBUG: KafkaProtocolComponents instantiated");
        }

        public Function1<Session, Session> onStart() {
            kafkaProtocol.getRequestStore().startTimeoutMonitoring(new pl.perfluencer.kafka.cache.TimeoutHandler() {
                @Override
                public void onTimeout(String correlationId, Map<String, Object> requestData) {
                    long startTime = Long.parseLong((String) requestData.get(RequestStore.START_TIME));
                    long endTime = System.currentTimeMillis();
                    String transactionName = (String) requestData.get(RequestStore.TRANSACTION_NAME);
                    String scenarioName = (String) requestData.get(RequestStore.SCENARIO_NAME);

                    coreComponents.statsEngine().logResponse(
                            scenarioName,
                            scala.collection.immutable.List$.MODULE$.empty(),
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
                    coreComponents.statsEngine().logResponse(
                            "Kafka Metrics",
                            scala.collection.immutable.List$.MODULE$.empty(),
                            "kafka-consumer-lag-max",
                            now - (long) maxLag, // Start time = now - lag, so Duration = lag
                            now,
                            io.gatling.commons.stats.Status.apply("OK"),
                            scala.Option.empty(),
                            scala.Option.empty());

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, 0, kafkaProtocol.getMetricInjectionInterval().toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        public Function1<Session, BoxedUnit> onExit() {
            return session -> {
                kafkaProtocol.getRequestStore().stopTimeoutMonitoring();
                if (metricScheduler != null) {
                    metricScheduler.shutdownNow();
                }
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
            requestStore = new InMemoryRequestStore(timeoutCheckInterval.toMillis());
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
                measureStoreLatency);
    }
}
