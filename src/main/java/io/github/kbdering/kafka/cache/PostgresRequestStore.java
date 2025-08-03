
package io.github.kbdering.kafka.cache;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import io.github.kbdering.kafka.SerializationType;

public class PostgresRequestStore implements RequestStore {

    private final DataSource dataSource;

    public PostgresRequestStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void storeRequest(String correlationId, String key, byte[] valueBytes, SerializationType serializationType, String transactionName, long startTime, long timeoutMillis) {
        // Note: The 'requests' table needs a 'request_value_bytes BYTEA' column
        // and a 'serialization_type VARCHAR(50)' (or similar) column.
        // The 'start_time' also needs to be stored if it's not already.
        // The 'timeoutMillis' is not directly used here but is part of the interface.
        String sql = "INSERT INTO requests (correlation_id, request_key, request_value_bytes, serialization_type, transaction_name, start_time) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setObject(1, UUID.fromString(correlationId)); // Use setObject for UUID
            pstmt.setString(2, key);
            pstmt.setBytes(3, valueBytes);
            pstmt.setString(4, serializationType.name()); // Store enum name as String
            pstmt.setString(5, transactionName);
            pstmt.setTimestamp(6, new Timestamp(startTime)); // Store startTime
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Error storing request in PostgreSQL", e);
        }
    }


    
    @Override
    public Map<String, Object> getRequest(String correlationId) {
        // Ensure your table has 'request_value_bytes BYTEA' and 'serialization_type VARCHAR(50)'
        String sql = "SELECT request_key, request_value_bytes, serialization_type, transaction_name, start_time FROM requests WHERE correlation_id = ? AND expired = FALSE FOR UPDATE SKIP LOCKED";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setObject(1, UUID.fromString(correlationId)); // Assuming correlationId is a UUID string
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Map<String, Object> data = new HashMap<>();
                    data.put(InMemoryRequestStore.KEY, rs.getString("request_key"));
                    data.put(InMemoryRequestStore.VALUE_BYTES, rs.getBytes("request_value_bytes"));
                    data.put(InMemoryRequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(rs.getString("serialization_type")));
                    data.put(InMemoryRequestStore.TRANSACTION_NAME, rs.getString("transaction_name"));
                    data.put(InMemoryRequestStore.START_TIME, String.valueOf(rs.getTimestamp("start_time").getTime()));
                    return data;
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException("Error getting request from PostgreSQL", e);
        }
        return null; // Or throw an exception if not found is an error
    }


    public Map<String, Map<String, Object>> getRequests(List<String> correlationIds) {
        // Return early if input list is invalid
        if (correlationIds == null || correlationIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Map<String, Object>> foundRequests = new HashMap<>();
        // Ensure your table has 'request_value_bytes BYTEA' and 'serialization_type VARCHAR(50)'
        String sql = "SELECT correlation_id, request_key, request_value_bytes, serialization_type, transaction_name, start_time " +
                "FROM requests WHERE correlation_id = ANY(?) AND expired = FALSE";

        try (Connection conn = dataSource.getConnection()) {
            // Convert List<String> of UUIDs to UUID[] for PostgreSQL array
            UUID[] uuidArray = correlationIds.stream().map(UUID::fromString).toArray(UUID[]::new);
            Array sqlArray = conn.createArrayOf("uuid", uuidArray);

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setArray(1, sqlArray); // Set the array parameter

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> data = new HashMap<>();
                        String currentCorrelationId = rs.getObject("correlation_id").toString(); // Get UUID as string
                        data.put(InMemoryRequestStore.KEY, rs.getString("request_key"));
                        data.put(InMemoryRequestStore.VALUE_BYTES, rs.getBytes("request_value_bytes"));
                        data.put(InMemoryRequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(rs.getString("serialization_type")));
                        data.put(InMemoryRequestStore.TRANSACTION_NAME, rs.getString("transaction_name"));
                        Timestamp startTimeStamp = rs.getTimestamp("start_time");
                        data.put(InMemoryRequestStore.START_TIME, startTimeStamp != null ? String.valueOf(startTimeStamp.getTime()) : null);
                        foundRequests.put(currentCorrelationId, data);
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

    public void deleteRequests(List<String> correlationIds) {
        if (correlationIds == null || correlationIds.isEmpty()) {
            return;
        }
        String sql = "DELETE FROM requests WHERE correlation_id = ANY(?)";
        try (Connection conn = dataSource.getConnection()) {
            UUID[] uuidArray = correlationIds.stream().map(UUID::fromString).toArray(UUID[]::new);
            Array sqlArray = conn.createArrayOf("uuid", uuidArray);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setArray(1, sqlArray);
                pstmt.executeUpdate();
            } finally {
                if (sqlArray != null) {
                    try {
                        sqlArray.free();
                    } catch (SQLException e) {
                        System.err.println("Error freeing SQL Array: " + e.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error bulk deleting requests from PostgreSQL", e);
        }
    }

    @Override
    public void deleteRequest(String correlationId) {
        String sql = "DELETE FROM requests  WHERE correlation_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setObject(1, UUID.fromString(correlationId)); // Assuming correlationId is a UUID string
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

    @Override
    public void processBatchedRecords(Map<String, byte[]> records, BatchProcessor process) {
        if (records == null || records.isEmpty()) {
            return;
        }

        try (Connection conn = dataSource.getConnection())

 {
            // Use a CTE with DELETE ... RETURNING for efficiency
            String sql = "DELETE FROM requests r WHERE r.correlation_id = ANY(?) " +
                    "RETURNING r.correlation_id, r.request_key, r.request_value_bytes, r.serialization_type, r.transaction_name, r.start_time";

            Map<String, Map<String, Object>> foundRequests = new HashMap<>();
            // create array of correlation IDs from the headers

                    // Create array of UUIDs for the query
            UUID[] uuidArray = records.keySet().stream().map(UUID::fromString).toArray(UUID[]::new);
            Array sqlArray = conn.createArrayOf("uuid", uuidArray);

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setArray(1, sqlArray);

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String correlationId = rs.getObject("correlation_id").toString();
                        Map<String, Object> requestData = new HashMap<>();
                        requestData.put(InMemoryRequestStore.KEY, rs.getString("request_key"));
                        requestData.put(InMemoryRequestStore.VALUE_BYTES, rs.getBytes("request_value_bytes"));
                        requestData.put(InMemoryRequestStore.SERIALIZATION_TYPE, SerializationType.valueOf(rs.getString("serialization_type")));
                        requestData.put(InMemoryRequestStore.TRANSACTION_NAME, rs.getString("transaction_name"));
                        requestData.put(InMemoryRequestStore.START_TIME, String.valueOf(rs.getTimestamp("start_time").getTime()));
                        foundRequests.put(correlationId, requestData);
                    }
                }
            } finally {
                sqlArray.free();
            }

            // Process all records, distinguishing between matched (and now deleted) and unmatched
            for (Map.Entry<String, byte[]> recordEntry : records.entrySet()) {
                String correlationId = recordEntry.getKey();
                if (foundRequests.containsKey(correlationId)) {
                    process.onMatch(correlationId, foundRequests.get(correlationId), recordEntry.getValue());
                } else {
                    process.onUnmatched(correlationId, recordEntry.getValue());
                }
            }

    } catch (SQLException e) {
            throw new RuntimeException("Error processing batched records from PostgreSQL", e);
    }
}
}