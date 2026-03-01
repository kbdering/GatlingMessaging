# Data-Driven Testing (Feeders)

Realistic load testing requires varied, production-like payloads. The Gatling Kafka Extension seamlessly integrates with Gatling Feeders to drive tests from CSVs, Databases, or programmatically generated datasets.

## Feeding Data into Payloads

Since the `.value()` block expects a lambda exposing the Gatling `Session`, it is trivial to extract injected Feeder data to build realistic JSON payloads or hydrated Avro/POJO models.

### CSV Feeder Example

Create a CSV file at `src/test/resources/transaction_data.csv`:

```csv
accountId,amount,currency,customerName
ACC-001,1500.00,USD,John Smith
ACC-002,2500.50,EUR,Jane Doe
ACC-003,750.25,GBP,Bob Wilson
```

Use it in your simulation:

```java
import static io.gatling.javaapi.core.CoreDsl.*;

public class TransactionSimulation extends Simulation {
    {
        // Load CSV feeder - circular ensures we don't run out of test data
        FeederBuilder<String> csvFeeder = csv("transaction_data.csv").circular();
        
        ScenarioBuilder scn = scenario("Transaction Processing")
            .feed(csvFeeder)  // Inject CSV row into each user's Session context
            .exec(
                KafkaDsl.kafka("Transaction Request")
                    .requestReply()
                    .requestTopic("transaction-requests")
                    .responseTopic("transaction-responses")
                    .key(session -> session.getString("accountId"))  // Route key from CSV
                    .value(session -> String.format(
                        "{\"accountId\":\"%s\",\"amount\":%s,\"currency\":\"%s\"}",
                        session.getString("accountId"),
                        session.getString("amount"),
                        session.getString("currency")
                    ))
                    // Ensure you cast properly to match expected formats
                    .serializationType(String.class, String.class, SerializationType.STRING)
                    .timeout(10, TimeUnit.SECONDS)
            );
        
        setUp(scn.injectOpen(constantUsersPerSec(10).during(60)))
            .protocols(kafkaProtocol);
    }
}
```

### Random / Programmatic Feeder

Often you want continuously unique dynamic data on-the-fly without the hassle of generating a million-row CSV.

```java
FeederBuilder<Object> randomFeeder = Stream.of(
    Map.of( // Supplier logic executes continuously 
        "randomAmount", () -> String.format("%.2f", Math.random() * 10000),
        "randomAccountId", () -> "ACC-" + UUID.randomUUID().toString().substring(0, 8),
        "timestamp", () -> String.valueOf(System.currentTimeMillis())
    )
).toFeeder();

ScenarioBuilder scn = scenario("Dynamic Transactions")
    .feed(randomFeeder)
    .exec(
        kafka("Payment")
            .requestReply()
            .requestTopic("tx_in")
            .responseTopic("tx_out")
            .key(session -> session.getString("randomAccountId"))
            .value(session -> {
                // Example of building an Avro or POJO record
                Payment p = new Payment();
                p.setAccount(session.getString("randomAccountId"));
                p.setAmount((Double) session.get("randomAmount"));
                return p;
            })
    );
```

## Available Source Strategies

By default, Gatling throws an exception if your injection loops outlast the number of rows in your Feeder source. You must define a strategy:

| Strategy | Usage | Best For |
|----------|-------|----------|
| `.queue()` | Each record used exactly once. Test aborts when empty. | Strict, unique transactional state setup. |
| `.circular()` | Wraps back to the start when the end is reached. | Infinite endurance tests. |
| `.random()` | Randomly selects rows continuously. | Infinite load tests with unpredictable sequencing. |
| `.shuffle()` | Random order, used exactly once each. | Unique setup without predictable hot-spotting. |
