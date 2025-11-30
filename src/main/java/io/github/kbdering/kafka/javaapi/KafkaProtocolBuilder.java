
package io.github.kbdering.kafka.javaapi;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.routing.RoundRobinPool;
import io.gatling.core.CoreComponents;
import io.gatling.core.config.GatlingConfiguration;
import io.gatling.core.protocol.Protocol;
import io.gatling.core.session.Session;
import io.gatling.javaapi.core.ProtocolBuilder;
import io.github.kbdering.kafka.actors.KafkaConsumerActor;
import io.github.kbdering.kafka.actors.KafkaMessages;
import io.github.kbdering.kafka.actors.KafkaProducerActor;
import io.github.kbdering.kafka.MessageCheck;
import io.github.kbdering.kafka.actors.MessageProcessorActor;
import io.github.kbdering.kafka.extractors.CorrelationExtractor;
import io.github.kbdering.kafka.cache.InMemoryRequestStore;
import io.github.kbdering.kafka.cache.RequestStore;
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
    private CorrelationExtractor correlationExtractor;

    private Duration pollTimeout = Duration.ofMillis(100);

    public KafkaProtocolBuilder pollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    public KafkaProtocolBuilder correlationExtractor(CorrelationExtractor extractor) {
        this.correlationExtractor = extractor;
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

    public KafkaProtocolBuilder consumerProperties(Map<String, Object> props) {
        consumerProperties.putAll(props);
        return this;
    }

    public KafkaProtocolBuilder requestStore(RequestStore requestStore) {
        this.requestStore = requestStore;
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
        private ActorRef producerRouter;
        private final Map<String, ConsumerAndProcessor> consumerAndProcessorsByTopic = new ConcurrentHashMap<>();

        private KafkaProtocol(Map<String, Object> producerProperties, Map<String, Object> consumerProperties,
                ActorSystem actorSystem, int numProducers, int numConsumers, RequestStore requestStore,
                CorrelationExtractor correlationExtractor, Duration pollTimeout) {
            this.producerProperties = producerProperties;
            this.consumerProperties = consumerProperties;
            this.actorSystem = actorSystem;
            this.numProducers = numProducers;
            this.numConsumers = numConsumers;
            this.requestStore = requestStore;
            this.correlationExtractor = correlationExtractor;
            this.pollTimeout = pollTimeout;
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
    }

    public static final class KafkaProtocolComponents implements io.gatling.core.protocol.ProtocolComponents {
        private final KafkaProtocol kafkaProtocol;
        private final CoreComponents coreComponents;

        public KafkaProtocolComponents(KafkaProtocol kafkaProtocol, CoreComponents coreComponents) {
            this.kafkaProtocol = kafkaProtocol;
            this.coreComponents = coreComponents;
            if (kafkaProtocol.getProducerRouter() == null) {
                ActorRef producerRouter = kafkaProtocol.getActorSystem().actorOf(
                        new RoundRobinPool(kafkaProtocol.getNumProducers())
                                .props(KafkaProducerActor.props(kafkaProtocol.getProducerProperties())),
                        "kafkaProducerRouter-" + coreComponents.toString());
                kafkaProtocol.setProducerRouter(producerRouter);
            }
        }

        public Function1<Session, Session> onStart() {
            kafkaProtocol.getRequestStore().startTimeoutMonitoring(new io.github.kbdering.kafka.cache.TimeoutHandler() {
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
            return session -> session;
        }

        public Function1<Session, BoxedUnit> onExit() {
            return session -> {
                kafkaProtocol.getRequestStore().stopTimeoutMonitoring();
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
        Objects.requireNonNull(bootstrapServers, "bootstrapServers must not be set");
        Objects.requireNonNull(groupId, "groupId must be set for request-reply");

        if (actorSystem == null) {
            actorSystem = ActorSystem.create("GatlingKafkaSystem");
        }

        if (requestStore == null) {
            requestStore = new InMemoryRequestStore();
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
        consumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
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
                pollTimeout);
    }
}
