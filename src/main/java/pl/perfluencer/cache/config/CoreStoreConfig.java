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
