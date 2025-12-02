package io.github.kbdering.kafka.benchmark;

import io.github.kbdering.kafka.cache.InMemoryRequestStore;
import io.github.kbdering.kafka.util.SerializationType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(0)
public class InMemoryRequestStoreBenchmark {

    private InMemoryRequestStore store;
    private static final long TIMEOUT_MS = 10;
    private static final long CHECK_INTERVAL_MS = 10;

    @Setup(Level.Trial)
    public void setup() {
        store = new InMemoryRequestStore(CHECK_INTERVAL_MS);
        store.startTimeoutMonitoring((correlationId, requestData) -> {
            // No-op handler
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        store.close();
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

        // 99% success (remove), 1% timeout (leave for background thread)
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

        // 1% success (remove), 99% timeout (leave for background thread)
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
