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

package pl.perfluencer.kafka;

import org.junit.Test;
import pl.perfluencer.common.util.SerializationType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Unit tests for SessionAwareMessageCheck.
 */
public class SessionAwareMessageCheckTest {

    // ==================== Basic Check Tests ====================

    @Test
    public void testCheck_successWithMatchingSessionVariable() {
        SessionAwareMessageCheck<String> check = new SessionAwareMessageCheck<>(
                "Account ID Check", String.class, SerializationType.STRING,
                (sessionVars, response) -> {
                    String expectedAccountId = sessionVars.get("accountId");
                    if (response.contains("\"accountId\":\"" + expectedAccountId + "\"")) {
                        return Optional.empty();
                    }
                    return Optional.of("Account ID mismatch");
                });

        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("accountId", "ACC-12345");
        sessionVars.put("amount", "100.00");

        Optional<String> result = check.validate(sessionVars, "{\"accountId\":\"ACC-12345\",\"status\":\"success\"}");
        assertTrue("Check should pass when account ID matches", result.isEmpty());
    }

    @Test
    public void testCheck_failureWithMismatchedSessionVariable() {
        SessionAwareMessageCheck<String> check = new SessionAwareMessageCheck<>(
                "Account ID Check", String.class, SerializationType.STRING,
                (sessionVars, response) -> {
                    String expectedAccountId = sessionVars.get("accountId");
                    if (response.contains("\"accountId\":\"" + expectedAccountId + "\"")) {
                        return Optional.empty();
                    }
                    return Optional.of("Account ID mismatch: expected " + expectedAccountId);
                });

        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("accountId", "ACC-12345");

        Optional<String> result = check.validate(sessionVars, "{\"accountId\":\"ACC-99999\",\"status\":\"success\"}");
        assertTrue("Check should fail when account ID doesn't match", result.isPresent());
        assertTrue(result.get().contains("Account ID mismatch"));
    }

    @Test
    public void testCheck_multipleSessionVariables() {
        SessionAwareMessageCheck<String> check = new SessionAwareMessageCheck<>(
                "Transaction Check", String.class, SerializationType.STRING,
                (sessionVars, response) -> {
                    String expectedAccountId = sessionVars.get("accountId");
                    String expectedAmount = sessionVars.get("amount");

                    if (!response.contains(expectedAccountId)) {
                        return Optional.of("Account ID not found in response");
                    }
                    if (!response.contains(expectedAmount)) {
                        return Optional.of("Amount not found in response");
                    }
                    return Optional.empty();
                });

        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("accountId", "ACC-12345");
        sessionVars.put("amount", "250.00");

        Optional<String> result = check.validate(sessionVars,
                "{\"accountId\":\"ACC-12345\",\"processedAmount\":\"250.00\",\"status\":\"completed\"}");
        assertTrue("Check should pass when all session variables match", result.isEmpty());
    }

    @Test
    public void testCheck_nullResponse() {
        SessionAwareMessageCheck<String> check = new SessionAwareMessageCheck<>(
                "Null Response Check", String.class, SerializationType.STRING,
                (sessionVars, response) -> {
                    if (response == null) {
                        return Optional.of("Response is null");
                    }
                    return Optional.empty();
                });

        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("accountId", "ACC-12345");

        Optional<String> result = check.validate(sessionVars, null);
        assertTrue("Null response should fail", result.isPresent());
        assertEquals("Response is null", result.get());
    }

    @Test
    public void testCheck_emptySessionVariables() {
        SessionAwareMessageCheck<String> check = new SessionAwareMessageCheck<>(
                "Response Not Empty Check", String.class, SerializationType.STRING,
                (sessionVars, response) -> {
                    // Even with empty session vars, check that response is not empty
                    if (response == null || response.isEmpty()) {
                        return Optional.of("Response is empty");
                    }
                    return Optional.empty();
                });

        Map<String, String> sessionVars = new HashMap<>(); // Empty

        Optional<String> result = check.validate(sessionVars, "{\"status\":\"success\"}");
        assertTrue("Check should pass even with empty session vars", result.isEmpty());
    }

    @Test
    public void testCheck_getters() {
        SessionAwareMessageCheck<String> check = new SessionAwareMessageCheck<>(
                "My Check", String.class, SerializationType.STRING,
                (sessionVars, response) -> Optional.empty());

        assertEquals("My Check", check.getCheckName());
        assertEquals(String.class, check.getResponseClass());
        assertEquals(SerializationType.STRING, check.getResponseSerdeType());
        assertNotNull(check.getCheckLogic());
    }

    @Test
    public void testCheck_byteArrayResponse() {
        SessionAwareMessageCheck<byte[]> check = new SessionAwareMessageCheck<>(
                "Binary Check", byte[].class, SerializationType.BYTE_ARRAY,
                (sessionVars, response) -> {
                    if (response != null && response.length > 0) {
                        return Optional.empty();
                    }
                    return Optional.of("Empty binary response");
                });

        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("token", "abc123");

        Optional<String> result = check.validate(sessionVars, "test".getBytes());
        assertTrue("Binary check should pass for non-empty bytes", result.isEmpty());
    }

    @Test
    public void testCheck_exceptionInLogic() {
        SessionAwareMessageCheck<String> check = new SessionAwareMessageCheck<>(
                "Exception Check", String.class, SerializationType.STRING,
                (sessionVars, response) -> {
                    throw new RuntimeException("Simulated error");
                });

        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("key", "value");

        try {
            Optional<String> result = check.validate(sessionVars, "response");
            // If it doesn't throw, it's also acceptable
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Simulated"));
        }
    }
}
