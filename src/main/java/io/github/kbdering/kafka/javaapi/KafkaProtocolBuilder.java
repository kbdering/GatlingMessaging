
package io.github.kbdering.kafka.javaapi;

import akka.actor.ActorSystem;
import io.gatling.core.CoreComponents;
import io.gatling.core.config.GatlingConfiguration;
import io.gatling.core.protocol.Protocol;
import io.gatling.core.session.Session;
import io.gatling.javaapi.core.ProtocolBuilder;
import io.github.kbdering.kafka.MessageCheck;
import io.github.kbdering.kafka.cache.InMemoryRequestStore;
import io.github.kbdering.kafka.cache.RedisRequestStore;
import io.github.kbdering.kafka.cache.RequestStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer; // Keep for Key
import org.apache.kafka.common.serialization.StringSerializer; // Keep for Key
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*; // Import List and Collections
import java.util.Map;
import java.util.Objects;

import io.gatling.core.protocol.ProtocolKey; // Import ProtocolKey
import scala.Function1;
import scala.runtime.BoxedUnit;


public final class KafkaProtocolBuilder implements ProtocolBuilder {

    private final Map<String, Object> producerProperties = new HashMap<>();
    private final Map<String, Object> consumerProperties = new HashMap<>();
    private String bootstrapServers = null;
    private String groupId = null;
    private ActorSystem actorSystem = null;
    private int numProducers = 0;
    private int numConsumers = 0;

    private RequestStore requestStore;



    public KafkaProtocolBuilder bootstrapServers(String servers) {
        Objects.requireNonNull(servers, "bootstrapServers must not be null");
        this.bootstrapServers = servers;
        return this;
    }

    public KafkaProtocolBuilder actorSystem(ActorSystem system) {
        Objects.requireNonNull(system, "actorSystem must not be null");
        this.actorSystem = system;
        return this;
    }

    public KafkaProtocolBuilder numProducers(int numProducers) {
        if (numProducers <= 0) {
            throw new IllegalArgumentException("numProducers must be greater than 0");
        }
        this.numProducers = numProducers;
        return this;
    }

    public KafkaProtocolBuilder numConsumers(int numConsumers) {
        if (numConsumers <= 0) {
            throw new IllegalArgumentException("numConsumers must be greater than 0");
        }
        this.numConsumers = numConsumers;
        return this;
    }

    public KafkaProtocolBuilder groupId(String groupId) {
        Objects.requireNonNull(groupId, "groupId must not be null");
        this.groupId = groupId;
        return this;
    }

    public KafkaProtocolBuilder producerProperty(String key, Object value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        producerProperties.put(key, value);
        return this;
    }

    public KafkaProtocolBuilder producerProperties(Map<String, Object> props) {
        Objects.requireNonNull(props, "props must not be null");
        producerProperties.putAll(props);
        return this;
    }

    public KafkaProtocolBuilder consumerProperty(String key, Object value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        consumerProperties.put(key, value);
        return this;
    }

    public KafkaProtocolBuilder consumerProperties(Map<String, Object> props) {
        Objects.requireNonNull(props, "props must not be null");
        consumerProperties.putAll(props);
        return this;
    }
    public KafkaProtocolBuilder requestStore(RequestStore requestStore) {
        this.requestStore = Objects.requireNonNull(requestStore, "requestStore must not be null");
        return this;
    }

    @Override
    public Protocol protocol() {
        return build();
    }

    public void setRequestStore(RequestStore requestStore) {
        this.requestStore = requestStore;
    }

    // Inner class to hold the actual protocol configuration
    public static class KafkaProtocol implements Protocol {
        private final Map<String, Object> producerProperties;
        private final Map<String, Object> consumerProperties;
        private final ActorSystem actorSystem;
        private final int numProducers;
        private final int numConsumers;
        private final RequestStore requestStore;

        // Private constructor to enforce creation through the builder
        private KafkaProtocol(Map<String, Object> producerProperties, Map<String, Object> consumerProperties, 
                              ActorSystem actorSystem, int numProducers, int numConsumers, RequestStore requestStore) { 
            this.producerProperties = producerProperties;
            this.consumerProperties = consumerProperties;
            this.actorSystem = actorSystem;
            this.numProducers = numProducers;
            this.numConsumers = numConsumers;
            this.requestStore = requestStore;
        }

        // Public getter methods for the configuration
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
    }
    // Inner class to represent protocol components
    public static final class KafkaProtocolComponents implements io.gatling.core.protocol.ProtocolComponents {

        private final KafkaProtocol kafkaProtocol;
        public static final ProtocolKey<KafkaProtocol,KafkaProtocolComponents> protocolKey = new ProtocolKey<KafkaProtocol,KafkaProtocolComponents>(){

            @Override
            public Class<io.gatling.core.protocol.Protocol> protocolClass() {
                return (Class<io.gatling.core.protocol.Protocol>) (Class<?>) KafkaProtocol.class; // Corrected
            }

            @Override
            public KafkaProtocol defaultProtocolValue(GatlingConfiguration configuration) {
                return new KafkaProtocolBuilder().bootstrapServers("localhost:9092").groupId("default-gatling-group").numProducers(1).numConsumers(1).build();
            }

            @Override
            public Function1<KafkaProtocol, KafkaProtocolComponents> newComponents(CoreComponents coreComponents) {
                return protocol -> new KafkaProtocolComponents(protocol);
            }


            public KafkaProtocolComponents protocolComponents(io.gatling.core.session.Session session) {
                throw new UnsupportedOperationException("Unimplemented method 'protocolComponents'");
            }

            public KafkaProtocol protocol(io.gatling.core.session.Session session) {
                throw new UnsupportedOperationException("Unimplemented method 'protocol'");
            }


        };
        public KafkaProtocolComponents(KafkaProtocol kafkaProtocol) {
            this.kafkaProtocol = kafkaProtocol;
        }

        public Protocol protocol() {
            return this.kafkaProtocol;
        }

        public ProtocolKey<KafkaProtocol, ?> key() {
            return this.protocolKey;

        }

        public KafkaProtocol protocol(io.gatling.core.session.Session session) {
            return this.kafkaProtocol;

        }

        public void start() {
            // TODO Auto-generated method stub

        }

        public void stop() {
            // TODO Auto-generated method stub

        }

        @Override
        public Function1<Session, Session> onStart() {
            return session -> session;
        }

        @Override
        public Function1<Session, BoxedUnit> onExit() {
            return session -> BoxedUnit.UNIT;
        }

        public scala.Option<KafkaProtocol> defaultProtocolValue(GatlingConfiguration configuration) {
            // Provide a default KafkaProtocol instance if none is configured.
            // This is crucial for late binding.  We create a *minimal*
            // KafkaProtocol here, enough to satisfy Gatling's internal checks.
            // The *actual* configuration will be used if the user provides one.
            return scala.Option.apply(new KafkaProtocolBuilder().bootstrapServers("localhost:9092").groupId("default-group").build());
        }
    }

    public KafkaProtocol build() {
        if (bootstrapServers == null) {
            throw new IllegalStateException("bootstrapServers must be set before building the protocol.");
        }
        if (groupId == null) {
            throw new IllegalStateException("groupId must be set for request-reply");
        }
        if (actorSystem == null) {
            actorSystem = ActorSystem.create("GatlingKafkaSystem");
        }

        if (requestStore == null) { // Use InMemory if not requestStore provided
            requestStore = new InMemoryRequestStore();

        }

        producerProperties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()); // For byte[]

        consumerProperties.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()); // For byte[]
        consumerProperties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProperties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        // Create the concrete KafkaProtocol instance
        return new KafkaProtocol(
                new HashMap<>(this.producerProperties),
                new HashMap<>(this.consumerProperties),
                this.actorSystem,
                this.numProducers,
                this.numConsumers,
                this.requestStore
        );

    }
}