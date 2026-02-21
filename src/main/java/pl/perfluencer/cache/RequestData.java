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

package pl.perfluencer.cache;

import pl.perfluencer.common.util.SerializationType;
import java.util.Map;

/**
 * A lightweight POJO to hold request data in the buffering queue.
 * Using this instead of HashMap reduces allocation overhead and boxing
 * during the critical path (storeRequest).
 */
public class RequestData {
    public final String correlationId;
    public final String key;
    public final Object value;
    public final SerializationType serializationType;
    public final String transactionName;
    public final String scenarioName;
    public final long startTime;
    public final long timeoutMillis;
    public final Map<String, String> sessionVariables;

    public RequestData(String correlationId, String key, Object value, SerializationType serializationType,
            String transactionName, String scenarioName, long startTime, long timeoutMillis,
            Map<String, String> sessionVariables) {
        this.correlationId = correlationId;
        this.key = key;
        this.value = value;
        this.serializationType = serializationType;
        this.transactionName = transactionName;
        this.scenarioName = scenarioName;
        this.startTime = startTime;
        this.timeoutMillis = timeoutMillis;
        this.sessionVariables = sessionVariables;
    }

}
