package pl.perfluencer.kafka.benchmark;

import pl.perfluencer.kafka.cache.RedisRequestStore;
import pl.perfluencer.kafka.util.SerializationType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(0)
public class RedisRequestStoreBenchmark {

    private GenericContainer<?> redis;
    private JedisPool jedisPool;
    private RedisRequestStore store;
    private static final long TIMEOUT_MS = 10;

    @Setup(Level.Trial)
    public void setup() {
        redis = new GenericContainer<>("redis:7.2.4")
                .withExposedPorts(6379);
        redis.start();

        redis.clients.jedis.JedisPoolConfig poolConfig = new redis.clients.jedis.JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);

        jedisPool = new JedisPool(poolConfig, redis.getHost(), redis.getMappedPort(6379));
        store = new RedisRequestStore(jedisPool);
        store.startTimeoutMonitoring((correlationId, requestData) -> {
            // No-op handler
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (store != null) {
            store.close();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        long counter = 0;
        String prefix = Thread.currentThread().getName() + "-";

        public String nextId() {
            return prefix + (++counter);
        }
    }

    @Benchmark
    public void noTimeout(ThreadState state, Blackhole bh) {
        String id = state.nextId();
        store.storeRequest(id, "key", "value", SerializationType.STRING,
                "txn", "scn", System.currentTimeMillis(), TIMEOUT_MS);

        // Immediately remove
        Map<String, Object> removed = store.getRequest(id);
        if (removed != null) {
            store.deleteRequest(id);
        }
        bh.consume(removed);
    }

    @Benchmark
    public void lowTimeout(ThreadState state, Blackhole bh) {
        String id = state.nextId();
        store.storeRequest(id, "key", "value", SerializationType.STRING,
                "txn", "scn", System.currentTimeMillis(), TIMEOUT_MS);

        // 99% success (remove), 1% timeout (leave for Redis TTL / background thread)
        if (ThreadLocalRandom.current().nextDouble() > 0.01) {
            Map<String, Object> removed = store.getRequest(id);
            if (removed != null) {
                store.deleteRequest(id);
            }
            bh.consume(removed);
        }
    }

    @Benchmark
    public void highTimeout(ThreadState state, Blackhole bh) {
        String id = state.nextId();
        store.storeRequest(id, "key", "value", SerializationType.STRING,
                "txn", "scn", System.currentTimeMillis(), TIMEOUT_MS);

        // 1% success (remove), 99% timeout (leave for Redis TTL / background thread)
        if (ThreadLocalRandom.current().nextDouble() > 0.99) {
            Map<String, Object> removed = store.getRequest(id);
            if (removed != null) {
                store.deleteRequest(id);
            }
            bh.consume(removed);
        }
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
