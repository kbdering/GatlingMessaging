package pl.perfluencer.kafka.benchmark;

import pl.perfluencer.cache.InMemoryRequestStore;
import pl.perfluencer.kafka.util.SerializationType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
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
    private static final long[] TIMEOUTS = { 10, 20, 30, 40 };
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
        long timeout = TIMEOUTS[ThreadLocalRandom.current().nextInt(TIMEOUTS.length)];
        store.storeRequest(id, "key", "value", SerializationType.STRING,
                "txn", "scn", System.currentTimeMillis(), timeout);

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
        long timeout = TIMEOUTS[ThreadLocalRandom.current().nextInt(TIMEOUTS.length)];
        store.storeRequest(id, "key", "value", SerializationType.STRING,
                "txn", "scn", System.currentTimeMillis(), timeout);

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
        long timeout = TIMEOUTS[ThreadLocalRandom.current().nextInt(TIMEOUTS.length)];
        store.storeRequest(id, "key", "value", SerializationType.STRING,
                "txn", "scn", System.currentTimeMillis(), timeout);

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
        // Run with increasing concurrency levels to measure scalability
        int[] concurrencyLevels = { 1, 4, 16, 64, 100 };

        for (int threads : concurrencyLevels) {
            System.out.println("Running with " + threads + " threads...");
            Options opt = new OptionsBuilder()
                    .include(InMemoryRequestStoreBenchmark.class.getSimpleName())
                    .forks(0)
                    .threads(threads)
                    .build();
            new Runner(opt).run();
        }
    }
}
