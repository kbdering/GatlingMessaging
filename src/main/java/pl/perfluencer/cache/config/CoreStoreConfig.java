package pl.perfluencer.cache.config;

import java.time.Duration;

/**
 * Configuration options for Request Stores in the Core module.
 */
public class CoreStoreConfig {

    // Generic settings for timeout checks
    private Duration timeoutCheckInterval = Duration.ofSeconds(10);
    private int timeoutBatchSize = 1000;

    public CoreStoreConfig() {
    }

    public CoreStoreConfig timeoutCheckInterval(Duration timeoutCheckInterval) {
        this.timeoutCheckInterval = timeoutCheckInterval;
        return this;
    }

    public Duration getTimeoutCheckInterval() {
        return timeoutCheckInterval;
    }

    public CoreStoreConfig timeoutBatchSize(int timeoutBatchSize) {
        this.timeoutBatchSize = timeoutBatchSize;
        return this;
    }

    public int getTimeoutBatchSize() {
        return timeoutBatchSize;
    }
}
