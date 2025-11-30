# Getting Started with Docker Compose

This guide walks you through setting up the complete testing environment using Docker Compose.

## Step-by-Step Setup

### 1. Generate TLS Certificates

```bash
cd /path/to/GatlingJava
chmod +x scripts/generate-tls-certs.sh
./scripts/generate-tls-certs.sh
```

**What this does:**
- Creates a Certificate Authority (CA)
- Generates broker keystore and truststore
- Creates client truststore for Gatling
- All files are stored in `tls-certs/` directory

### 2. Start Services

```bash
docker-compose up -d
```

**Services started:**
- Zookeeper (port 2181)
- Kafka with PLAINTEXT (9092) and TLS (9093)
- Redis (6379)
- PostgreSQL (5432)
- Kafka UI (8080)

### 3. Verify Health

```bash
# Check all services are running
docker-compose ps

# Test Kafka connection
docker exec gatling-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Test Redis
docker exec gatling-redis redis-cli ping

# Test Postgres
docker exec gatling-postgres pg_isready -U gatling
```

### 4. Initialize Database Schema

```bash
# Connect to PostgreSQL and run schema
docker exec -i gatling-postgres psql -U gatling -d gatling < src/main/resources/sql/create_requests_table.sql
```

### 5. Create Kafka Topics

```bash
# Request topic (4 partitions for parallelism)
docker exec gatling-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic request_topic \
  --partitions 4 \
  --replication-factor 1

# Response topic
docker exec gatling-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic response_topic \
  --partitions 4 \
  --replication-factor 1

# Verify
docker exec gatling-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 6. Access Kafka UI

Open http://localhost:8080 in your browser to:
- View topics and messages
- Monitor consumer groups
- Check broker configuration

## Running Your First Test

### Option A: PLAINTEXT Connection

```java
KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9092")
    .groupId("gatling-test-group")
    .numProducers(1)
    .numConsumers(4);
```

### Option B: TLS Connection

```java
import org.apache.kafka.common.config.SslConfigs;

KafkaProtocolBuilder protocol = kafka()
    .bootstrapServers("localhost:9093") // TLS port
    .groupId("gatling-test-group")
    .producerProperties(Map.of(
        "security.protocol", "SSL",
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "./tls-certs/kafka.client.truststore.jks",
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafkatest123",
        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""
    ))
    .consumerProperties(Map.of(
        "security.protocol", "SSL",
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "./tls-certs/kafka.client.truststore.jks",
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafkatest123",
        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""
    ));
```

## Troubleshooting

### Kafka Won't Start

```bash
# Check logs
docker-compose logs kafka

# Common issue: Port already in use
lsof -i :9092
# Kill the process or use different ports
```

### TLS Handshake Failures

```bash
# Verify certificates exist
ls -la tls-certs/

# Regenerate if needed
rm -rf tls-certs/*.jks tls-certs/ca-* tls-certs/broker-*
./scripts/generate-tls-certs.sh
```

### Redis Connection Refused

```bash
# Check Redis is running
docker exec gatling-redis redis-cli ping

# Should return: PONG
```

### PostgreSQL Connection Failed

```bash
# Check database exists
docker exec gatling-postgres psql -U gatling -l

# Recreate database
docker-compose down -v
docker-compose up -d postgres
```

## Cleanup

```bash
# Stop services (keep data)
docker-compose stop

# Remove containers (keep data)
docker-compose down

# Remove everything including volumes
docker-compose down -v
```

## Production Considerations

For production deployments:

1. **Change default passwords** in `docker-compose.yml`
2. **Use external certificate authority** instead of self-signed certs
3. **Enable Kafka authentication** (SASL/SCRAM)
4. **Configure Redis persistence** if needed
5. **Use environment variables** for secrets
6. **Set up monitoring** (Prometheus + Grafana)

Example production environment file (`.env`):
```bash
KAFKA_PASSWORD=<strong-password>
POSTGRES_PASSWORD=<strong-password>
REDIS_PASSWORD=<strong-password>
TLS_KEYSTORE_PASSWORD=<strong-password>
```
