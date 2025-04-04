
package io.github.kbdering.kafka.cache;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class PostgresRequestStore implements RequestStore {

    private final DataSource dataSource;

    public PostgresRequestStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void storeRequest(String correlationId, String key, String value, String transactionName, long startTime, long timeoutMillis) {
        String sql = "INSERT INTO requests (correlation_id, request_key, request_value, transaction_name) VALUES (?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setObject(1, UUID.fromString(correlationId)); // Use setObject for UUID
            pstmt.setString(2, key);
            pstmt.setString(3, value);
            pstmt.setString(4, transactionName);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Error storing request in PostgreSQL", e);
        }
    }
    @Override
    public Map<String, String> getRequest(String correlationId) {
        String sql = "SELECT request_key, request_value, transaction_name, start_time FROM requests WHERE correlation_id = ? AND expired = FALSE FOR UPDATE";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setObject(1, correlationId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Map<String, String> data = new HashMap<>();
                    data.put("key", rs.getString("request_key"));
                    data.put("value", rs.getString("request_value"));
                    data.put("transactionName", rs.getString("transaction_name"));
                    data.put("startTime", String.valueOf(rs.getLong("start_time")));
                    return data;
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException("Error getting request from PostgreSQL", e);
        }
        return null; // Or throw an exception if not found is an error
    }


    public Map<String, Map<String, String>> getRequests(List<String> correlationIds) {
        // Return early if input list is invalid
        if (correlationIds == null || correlationIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Map<String, String>> foundRequests = new HashMap<>();
        String sql = "SELECT correlation_id, request_key, request_value, transaction_name, start_time " +
                "FROM requests WHERE correlation_id = ANY(?) AND expired = FALSE";

        try (Connection conn = dataSource.getConnection()) {
            String[] correlationIdArray = (String[])correlationIds.toArray();
            Array sqlArray = conn.createArrayOf("string", correlationIdArray);

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setArray(1, sqlArray); // Set the array parameter

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, String> data = new HashMap<>();
                        String correlationId = rs.getString("correlation_id");
                        data.put("key", rs.getString("request_key"));
                        data.put("value", rs.getString("request_value"));
                        data.put("transactionName", rs.getString("transaction_name"));
                        Timestamp startTimeStamp = rs.getTimestamp("start_time");
                        data.put("startTime", startTimeStamp != null ? String.valueOf(startTimeStamp.getTime()) : null);
                        foundRequests.put(correlationId, data);
                    }
                }
            } finally {
                // Free the SQL Array object's resources
                if (sqlArray != null) {
                    try {
                        sqlArray.free();
                    } catch (SQLException e) {
                        System.err.println("Error freeing SQL Array: " + e.getMessage());
                        // Log this error but don't necessarily rethrow over a primary exception
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error getting requests by IDs from PostgreSQL: " + e.getMessage());
            // Consider logging the specific IDs that caused the issue if possible (might require more complex error handling)
            throw new RuntimeException("Error getting requests by IDs from PostgreSQL", e);
        } catch (IllegalArgumentException e) {
            // Catch potential UUID.fromString errors
            System.err.println("Error parsing one or more correlation IDs as UUID: " + e.getMessage());
            throw new RuntimeException("Invalid UUID format in correlation ID list", e);
        }

        return foundRequests;
    }

    @Override
    public void deleteRequest(String correlationId) {
        String sql = "DELETE FROM requests  WHERE correlation_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setObject(1, correlationId);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException("Error updating request from PostgreSQL", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (dataSource instanceof AutoCloseable) {
            ((AutoCloseable) dataSource).close();
        }
    }
}