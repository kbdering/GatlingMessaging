package pl.perfluencer.kafka.benchmark;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import pl.perfluencer.kafka.cache.PostgresRequestStore;
import pl.perfluencer.kafka.util.SerializationType;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import org.apache.pekko.japi.pf.ReceiveBuilder;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class PostgresActorBenchmark {

    private static final int NUM_PRODUCERS = 1000;
    private static final int NUM_CONSUMERS = 32;
    private static final double TIMEOUT_RATE = 0.05;
    private static final Duration DURATION = Duration.ofSeconds(30);

    public static void main(String[] args) throws InterruptedException {
        // Start Postgres
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16.2")
                .withDatabaseName("gatling")
                .withUsername("gatling")
                .withPassword("gatling")
                .withInitScript("sql/create_partitioned_requests_table.sql")
                .withTmpFs(java.util.Collections.singletonMap("/var/lib/postgresql/data", "rw"))
                .withCommand("postgres",
                        "-c", "fsync=off",
                        "-c", "synchronous_commit=off",
                        "-c", "full_page_writes=off",
                        "-c", "max_connections=2000",
                        "-c", "shared_buffers=1GB",
                        "-c", "work_mem=32MB")) {
            postgres.start();

            // Configure HikariCP
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(postgres.getJdbcUrl());
            hikariConfig.setUsername(postgres.getUsername());
            hikariConfig.setPassword(postgres.getPassword());
            hikariConfig.setMaximumPoolSize(NUM_PRODUCERS + NUM_CONSUMERS + 50);
            hikariConfig.setMinimumIdle(50);
            hikariConfig.setAutoCommit(true); // Auto-commit for simplicity in benchmark

            HikariDataSource dataSource = new HikariDataSource(hikariConfig);

            PostgresRequestStore store = new PostgresRequestStore(dataSource);
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
            ActorSystem system = ActorSystem.create("PostgresBenchmarkSystem", config);

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
            dataSource.close();
        }
    }

    private static void runBenchmark(ActorSystem system, PostgresRequestStore store, int batchSize)
            throws InterruptedException {
        AtomicLong producedOps = new AtomicLong(0);
        AtomicLong consumedOps = new AtomicLong(0);
        Queue<String> messageQueue = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(NUM_PRODUCERS + NUM_CONSUMERS);

        System.out.println("Starting benchmark: " + NUM_PRODUCERS + " Producers, " + NUM_CONSUMERS
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

    static class ProducerActor extends AbstractActor {
        private final PostgresRequestStore store;
        private final CountDownLatch latch;
        private final long endTime;
        private final Queue<String> queue;
        private final AtomicLong producedOps;

        public static Props props(PostgresRequestStore store, CountDownLatch latch, long endTime, Queue<String> queue,
                AtomicLong producedOps) {
            return Props.create(ProducerActor.class,
                    () -> new ProducerActor(store, latch, endTime, queue, producedOps));
        }

        public ProducerActor(PostgresRequestStore store, CountDownLatch latch, long endTime, Queue<String> queue,
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

                            // 5% Timeout Rate: Drop the message (don't send to consumer)
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
        private final PostgresRequestStore store;
        private final CountDownLatch latch;
        private final long endTime;
        private final int batchSize;
        private final Queue<String> queue;
        private final AtomicLong consumedOps;

        public static Props props(PostgresRequestStore store, CountDownLatch latch, long endTime, int batchSize,
                Queue<String> queue, AtomicLong consumedOps) {
            return Props.create(ConsumerActor.class,
                    () -> new ConsumerActor(store, latch, endTime, batchSize, queue, consumedOps));
        }

        public ConsumerActor(PostgresRequestStore store, CountDownLatch latch, long endTime, int batchSize,
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
                            // Drain remaining? No, just stop for benchmark consistency
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
