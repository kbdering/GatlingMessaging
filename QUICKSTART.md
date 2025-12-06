# 🚀 Gatling Kafka Extension - Quick Reference


### What's New (2025-11-30)
- ✅ Docker Compose with TLS-enabled Kafka
- ✅ GitHub Actions CI/CD pipeline
- ✅ Connection pool sizing documentation

---

## Quick Start (5 minutes)

```bash
# 1. Setup environment
./scripts/generate-tls-certs.sh
docker-compose up -d

# 2. Create topics
docker exec gatling-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic request_topic --partitions 4 --replication-factor 1

docker exec gatling-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic response_topic --partitions 4 --replication-factor 1

# 3. Run tests
mvn clean test

# 4. Monitor (optional)
open http://localhost:8080  # Kafka UI
```

---

## 📚 Documentation

- **[README.md](README.md)** - Complete framework documentation
- **[DOCKER_SETUP.md](docs/DOCKER_SETUP.md)** - Docker environment guide
- **[CI Workflow](.github/workflows/ci.yml)** - GitHub Actions pipeline

---

## 🔌 Connection Examples

### PLAINTEXT (Development)
```java
kafka()
    .bootstrapServers("localhost:9092")
    .numProducers(1)
    .numConsumers(4);
```

### TLS (Production-like)
```java
kafka()
    .bootstrapServers("localhost:9093")
    .producerProperties(Map.of(
        "security.protocol", "SSL",
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, 
            "./tls-certs/kafka.client.truststore.jks",
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafkatest123"
    ))
    .consumerProperties(Map.of(
        "security.protocol", "SSL",
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, 
            "./tls-certs/kafka.client.truststore.jks",
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafkatest123"
    ));
```

---

## 🐳 Available Services

| Service | Endpoint | Credentials |
|---------|----------|-------------|
| Kafka (PLAINTEXT) | localhost:9092 | - |
| Kafka (TLS) | localhost:9093 | See tls-certs/ |
| Redis | localhost:6379 | - |
| PostgreSQL | localhost:5432 | gatling/password |
| Kafka UI | http://localhost:8080 | - |

---

## 🧪 Testing

```bash
# Unit tests only
mvn test -Dtest='!*IntegrationTest'

# Integration tests (requires Docker)
mvn test -Dtest='*IntegrationTest'

# All tests
mvn verify
```

---

## 📊 Connection Pool Sizing

### Redis (JedisPool)
| Scale | Users/sec | maxTotal |
|-------|-----------|----------|
| Small | < 10 | 10-20 |
| Medium | 10-100 | 30-50 |
| Large | 100-500 | 50-100 |

**Formula**: `(Users/sec) × (Duration) × 1.5`

### PostgreSQL (HikariCP)
| Scale | Concurrent | Pool Size |
|-------|-----------|-----------|
| Small | < 50/sec | 10-20 |
| Medium | 50-200/sec | 20-40 |
| Large | 200-500/sec | 40-80 |

**Formula**: `(Concurrent) × (Query time) + 5`

---

## 🔧 Troubleshooting

**Kafka won't start?**
```bash
docker-compose logs kafka
lsof -i :9092  # Check port conflicts
```

**TLS errors?**
```bash
./scripts/generate-tls-certs.sh  # Regenerate
```

**Database connection failed?**
```bash
docker exec -i gatling-postgres psql -U gatling -d gatling \
  < src/main/resources/sql/create_requests_table.sql
```

---

## 🎯 Best Practices

1. **Development**: Use PLAINTEXT (port 9092)
2. **Staging/Prod Testing**: Use TLS (port 9093)
3. **Load Tests**: Monitor connection pool metrics
4. **Cleanup**: `docker-compose down -v` removes all data

---

## 📞 Support

- Documentation: See [README.md](README.md)
- Issues: GitHub Issues
- CI Status: `.github/workflows/ci.yml`

**Happy Testing!** 🚀
