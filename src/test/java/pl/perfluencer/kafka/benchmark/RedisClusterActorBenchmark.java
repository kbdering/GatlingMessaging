package pl.perfluencer.kafka.benchmark;

import pl.perfluencer.kafka.cache.RedisClusterRequestStore;
import pl.perfluencer.kafka.util.SerializationType;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import org.apache.pekko.japi.pf.ReceiveBuilder;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.HostAndPort;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Testcontainers
public class RedisClusterActorBenchmark {

    // Using a known image that supports cluster configuration
    // Note: If RedisClusterContainer is not available in the classpath, we might
    // need a workaround.
    // Assuming 1.6.4 has it. If not, I'll see the compilation error.
    // Use GenericContainer with grokzen/redis-cluster to control ports and avoid
    // 7000 (AirPlay)
    // We map 8000-8005.
    @Container
    private static final org.testcontainers.containers.GenericContainer<?> KAFKA_REDIS = new org.testcontainers.containers.GenericContainer<>(
            org.testcontainers.utility.DockerImageName.parse("grokzen/redis-cluster:7.0.10"))
            .withEnv("IP", "127.0.0.1")
            .withEnv("INITIAL_PORT", "8000")
            .withEnv("MASTERS", "3")
            .withEnv("SLAVES_PER_MASTER", "1")
            .withExposedPorts(8000, 8001, 8002, 8003, 8004, 8005);
    // We rely on Testcontainers random mapping? NO.
    // Redis Cluster needs fixed ports or NAT awareness.
    // Simplified approach: Bind to fixed host ports to ensure Client sees what
    // Server announces.
    // On Mac, we can try to rely on the fact that mapped ports match if we are
    // lucky? No.
    // We MUST use FixedHostPort (which is deprecated/discouraged) OR configured the
    // container to announce the mapped ports.
    // grokzen/redis-cluster is hard to configure for dynamic ports.
    // Recommended: Use setPortBindings.

    static {
        KAFKA_REDIS.setPortBindings(java.util.Arrays.asList(
                "8000:8000", "8001:8001", "8002:8002", "8003:8003", "8004:8004", "8005:8005"));
    }

    private static final int NUM_PRODUCERS = 100;
    private static final int NUM_CONSUMERS = 32;
    private static final double TIMEOUT_RATE = 0.05;
    private static final Duration DURATION = Duration.ofSeconds(30);

    public static void main(String[] args) throws InterruptedException {
        // Manually start if running main
        KAFKA_REDIS.start();

        try {
            // Configure JedisCluster
            Set<HostAndPort> nodes = new HashSet<>();
            // We know the ports are 8000-8005 on localhost
            nodes.add(new HostAndPort("127.0.0.1", 8000));

            JedisCluster jedisCluster = new JedisCluster(nodes);

            RedisClusterRequestStore store = new RedisClusterRequestStore(jedisCluster);
            store.startTimeoutMonitoring((id, data) -> {
            });

            String configString = "producer-dispatcher {\n" +
                    "  type = Dispatcher\n" +
                    "  executor = \"thread-pool-executor\"\n" +
                    "  thread-pool-executor {\n" +
                    "    fixed-pool-size = " + NUM_PRODUCERS + "\n" +
                    "  }\n" +
                    "  throughput = 1\n" +
                    "}\n" +
                    "consumer-dispatcher {\n" +
                    "  type = Dispatcher\n" +
                    "  executor = \"thread-pool-executor\"\n" +
                    "  thread-pool-executor {\n" +
                    "    fixed-pool-size = " + NUM_CONSUMERS + "\n" +
                    "  }\n" +
                    "  throughput = 1\n" +
                    "}";
            com.typesafe.config.Config config = com.typesafe.config.ConfigFactory.parseString(configString);
            ActorSystem system = ActorSystem.create("RedisClusterBenchmark", config);

            int[] batchSizes = { 100, 500 };

            for (int batchSize : batchSizes) {
                runBenchmark(system, store, batchSize);
            }

            system.terminate();
            try {
                store.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        } finally {
            KAFKA_REDIS.stop();
        }
    }

    private static void runBenchmark(ActorSystem system, RedisClusterRequestStore store, int batchSize)
            throws InterruptedException {
        AtomicLong producedOps = new AtomicLong(0);
        AtomicLong consumedOps = new AtomicLong(0);
        Queue<String> messageQueue = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(NUM_PRODUCERS + NUM_CONSUMERS);

        System.out.println("Starting Redis Cluster benchmark: " + NUM_PRODUCERS + " Producers, " + NUM_CONSUMERS
                + " Consumers, Batch Size " + batchSize + "...");
        long startTime = System.currentTimeMillis();
        long endTime = startTime + DURATION.toMillis();

        // Start Consumers
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            system.actorOf(ConsumerActor.props(store, latch, endTime, batchSize, messageQueue, consumedOps)
                    .withDispatcher("consumer-dispatcher"));
        }

        // Start Producers
        for (int i = 0; i < NUM_PRODUCERS; i++) {
            system.actorOf(ProducerActor.props(store, latch, endTime, messageQueue, producedOps)
                    .withDispatcher("producer-dispatcher"));
        }

        latch.await();
        long actualEndTime = System.currentTimeMillis();
        long totalTimeMs = actualEndTime - startTime;

        double producedPerSec = (double) producedOps.get() / (totalTimeMs / 1000.0);
        double consumedPerSec = (double) consumedOps.get() / (totalTimeMs / 1000.0);

        System.out.println("Batch Size " + batchSize + " Finished.");
        System.out.println("Produced: " + producedOps.get() + " (" + String.format("%.2f", producedPerSec) + " ops/s)");
        System.out.println("Consumed: " + consumedOps.get() + " (" + String.format("%.2f", consumedPerSec) + " ops/s)");
        System.out.println("Total Time: " + totalTimeMs + " ms");
        System.out.println("--------------------------------------------------");
    }

    // Reuse Actor definitions or duplicate small inner classes
    static class ProducerActor extends AbstractActor {
        private final RedisClusterRequestStore store;
        private final CountDownLatch latch;
        private final long endTime;
        private final Queue<String> queue;
        private final AtomicLong producedOps;

        public static Props props(RedisClusterRequestStore store, CountDownLatch latch, long endTime,
                Queue<String> queue,
                AtomicLong producedOps) {
            return Props.create(ProducerActor.class,
                    () -> new ProducerActor(store, latch, endTime, queue, producedOps));
        }

        public ProducerActor(RedisClusterRequestStore store, CountDownLatch latch, long endTime, Queue<String> queue,
                AtomicLong producedOps) {
            this.store = store;
            this.latch = latch;
            this.endTime = endTime;
            this.queue = queue;
            this.producedOps = producedOps;
        }

        @Override
        public void preStart() {
            self().tell("run", self());
        }

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                    .matchEquals("run", msg -> {
                        if (System.currentTimeMillis() < endTime) {
                            String id = UUID.randomUUID().toString();
                            store.storeRequest(id, "key", "value", SerializationType.STRING,
                                    "txn", "scn", System.currentTimeMillis(), 10000);

                            if (ThreadLocalRandom.current().nextDouble() >= TIMEOUT_RATE) {
                                queue.offer(id);
                            }

                            producedOps.incrementAndGet();
                            self().tell("run", self());
                        } else {
                            latch.countDown();
                            context().stop(self());
                        }
                    })
                    .build();
        }
    }

    static class ConsumerActor extends AbstractActor {
        private final RedisClusterRequestStore store;
        private final CountDownLatch latch;
        private final long endTime;
        private final int batchSize;
        private final Queue<String> queue;
        private final AtomicLong consumedOps;

        public static Props props(RedisClusterRequestStore store, CountDownLatch latch, long endTime, int batchSize,
                Queue<String> queue, AtomicLong consumedOps) {
            return Props.create(ConsumerActor.class,
                    () -> new ConsumerActor(store, latch, endTime, batchSize, queue, consumedOps));
        }

        public ConsumerActor(RedisClusterRequestStore store, CountDownLatch latch, long endTime, int batchSize,
                Queue<String> queue, AtomicLong consumedOps) {
            this.store = store;
            this.latch = latch;
            this.endTime = endTime;
            this.batchSize = batchSize;
            this.queue = queue;
            this.consumedOps = consumedOps;
        }

        @Override
        public void preStart() {
            self().tell("run", self());
        }

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                    .matchEquals("run", msg -> {
                        if (System.currentTimeMillis() < endTime) {
                            processBatch();
                            self().tell("run", self());
                        } else {
                            latch.countDown();
                            context().stop(self());
                        }
                    })
                    .build();
        }

        private void processBatch() {
            Map<String, Object> batch = new HashMap<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                String id = queue.poll();
                if (id == null)
                    break;
                // Emulate response
                batch.put(id, "response_value");
            }

            if (!batch.isEmpty()) {
                store.processBatchedRecords(batch, new pl.perfluencer.kafka.cache.BatchProcessor() {
                    @Override
                    public void onMatch(String correlationId, Map<String, Object> requestData, Object responseValue) {
                    }

                    @Override
                    public void onUnmatched(String correlationId, Object responseValue) {
                    }
                });
                consumedOps.addAndGet(batch.size());
            }
        }
    }
}
