package pl.perfluencer.kafka.integration;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Utility to load Testcontainers environment configuration from a local file.
 * This avoids hardcoding machine-specific host values in the source code.
 */
public class TestConfig {
    private static boolean initialized = false;

    public static synchronized void init() {
        if (initialized) return;

        Path envPath = Paths.get(".env.testcontainers");
        if (Files.exists(envPath)) {
            Properties props = new Properties();
            try (FileInputStream fis = new FileInputStream(envPath.toFile())) {
                props.load(fis);

                // Map standardized ENV names to internal Simulation property names
                mapProperty(props, "KAFKA_BOOTSTRAP_SERVERS", "kafka.bootstrap.servers");
                mapProperty(props, "SCHEMA_REGISTRY_URL", "schema.registry.url");
                mapProperty(props, "REDIS_URL", "redis.url");
                mapProperty(props, "POSTGRES_JDBC_URL", "postgres.jdbc.url");
                mapProperty(props, "BLNK_API_URL", "blnk.api.url");
                mapProperty(props, "BLNK_JDBC_URL", "blnk.jdbc.url");

                // Also load everything else directly as system properties (DOCKER_HOST, etc.)
                props.forEach((key, value) -> {
                    System.setProperty((String) key, (String) value);
                });
            } catch (IOException e) {
                System.err.println("Failed to load .env.testcontainers: " + e.getMessage());
            }
        }
        initialized = true;
    }

    private static void mapProperty(Properties props, String envKey, String sysKey) {
        String value = props.getProperty(envKey);
        if (value != null) {
            System.setProperty(sysKey, value);
        }
    }
}
