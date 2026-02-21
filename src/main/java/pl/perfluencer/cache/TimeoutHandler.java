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

import java.util.Map;

/**
 * Interface for handling timeout events for requests that have exceeded their
 * timeout period.
 */
public interface TimeoutHandler {
    /**
     * Called when a request has timed out.
     * 
     * @param correlationId The correlation ID of the timed out request
     * @param requestData   The original request data that timed out
     */
    void onTimeout(String correlationId, RequestData requestData);
}
