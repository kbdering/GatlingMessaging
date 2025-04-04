package io.github.kbdering.kafka;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.github.kbdering.kafka.cache.InMemoryRequestStore;
import io.github.kbdering.kafka.cache.PostgresRequestStore;
import io.github.kbdering.kafka.cache.RequestStore;
import io.github.kbdering.kafka.javaapi.KafkaDsl;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import javax.sql.DataSource;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.gatling.javaapi.core.CoreDsl.*;

public class KafkaRequestReplySimulation extends Simulation {

    {
        KafkaProtocolBuilder kafkaProtocol = KafkaDsl.kafka()
                .bootstrapServers("localhost:9092")
                .groupId("gatling-consumer-group")
                .numProducers(3)
                .numConsumers(2)
                .producerProperties(Map.of(
                        ProducerConfig.ACKS_CONFIG, "all"
                ));

        // SQL Pool using Hikari, Redis is also an option
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/yourdb");
        config.setUsername("youruser");
        config.setPassword("yourpassword");
        config.setDriverClassName("org.postgresql.Driver");
        config.setMaximumPoolSize(200);
        DataSource dataSource = new HikariDataSource(config);
        //RequestStore postgresStore = new PostgresRequestStore(dataSource);

        //Using inMemory cache for a single-server test
        RequestStore inMemoryStore = new InMemoryRequestStore();
        kafkaProtocol.setRequestStore(inMemoryStore);


        ScenarioBuilder scn = scenario("Kafka Request-Reply with Akka")
                .exec(
                        KafkaDsl.kafkaRequestReply("request_topic",
                                "request_topic",
                                session -> UUID.randomUUID().toString(),
                                "Kuba",
                                kafkaProtocol.protocol(),
                                10, TimeUnit.SECONDS)
                );

        setUp(
                scn.injectOpen(
                        rampUsersPerSec(10).to(1000).during(60),
                        constantUsersPerSec(1000).during(60),
                        rampUsersPerSec(1000).to(10).during(60),
                        // wait for any potential timeouts
                        nothingFor(Duration.ofSeconds(20))
                )
        ).protocols(kafkaProtocol);


    }
}