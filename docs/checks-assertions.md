# Assertions & Validation

Validating that your highly concurrent system returns the *correct* data under heavy load is just as important as measuring how fast it acts. The Gatling Kafka Extension provides a robust Gatling-style Fluent API for validating responses.

## Sub-Request Check Scoping

Checks defined within a `requestReply` block are **strictly and perfectly scoped** to that specific transaction instance logic. There are no "Global Catch-All" bugs.

```java
// Request A expects "OK"
kafka("CreateUser")
    .requestReply()
    .check(jsonPath("$.status").is("OK"))

// Request B expects "CREATED"
kafka("CreateAccount")
    .requestReply()
    .check(jsonPath("$.status").is("CREATED"))
```
When a response matching CreateAccount arrives, it will *only* run the `CREATED` assertions.

---

## 1. Fluent Check API

The recommended approach aligns flawlessly with Gatling's core HTTP DSL principles: **extractor → find strategy → [transform] → validator**.

Import the checks statically from the Commons library:
```java
import static pl.perfluencer.common.checks.Checks.*;
```

### POJO & Protobuf Field Extraction
If your deserializer returns typed Java Objects (like Protobuf generated models or internal POJOs), you can chain method references directly to avoid Reflection and JSON parsing overhead entirely.
```java
kafka("Typed Response")
    // ...
    .check(
        field(OrderResponse.class).get(OrderResponse::getStatus).is("OK"),
        field(OrderResponse.class).get(OrderResponse::getTotalAmount).gt(100.0)
    )
```

### JSONPath Extraction
Powerful inspection of complex JSON structures using `jsonPath()`. The Gatling JSONPath engine supports the standard specification, allowing for deep structural queries.

**Basic Existence and Equality:**
```java
kafka("Transaction Status")
    .requestReply()
    // ...
    .check(
        // Ensure 'status' field exists and equals "OK"
        jsonPath("$.status").find().is("OK"),
        
        // Check if an error object does NOT exist
        jsonPath("$.error").notExists()
    )
```

**Type Casting and Numeric Validation:**
JSONPath extracts Strings by default. Use `.of[Type]()` to validate numbers safely.
```java
    .check(
        // Extract as Double and ensure it's greater than or equal to 100
        jsonPath("$.amount").ofDouble().gte(100.0),
        
        // Extract as Integer and ensure it is strictly less than 5
        jsonPath("$.retryCount").ofInt().lt(5),
        
        // Extract boolean flag
        jsonPath("$.isActive").ofBoolean().is(true)
    )
```

**Arrays and Collection Logic:**
```java
    .check(
        // Verify the exact size of the 'items' array
        jsonPath("$.items[*]").count().is(3),
        
        // Extract all elements and verify the list contains "Widget"
        jsonPath("$.items[*]").findAll().satisfies(items -> items.contains("Widget")),
        
        // Save the first item into the Gatling Session for later use
        jsonPath("$.items[0].id").find().saveAs("firstItemId")
    )
```

### JSON Helpers
There are several high-speed shortcuts for common JSON evaluations, particularly useful for API structures.
```java
    .check(
        // Automatically checks for common success indicators (e.g. { "status":"success" }, { "success": true }, no "error" key)
        jsonSuccess(),
        
        jsonPathGreaterThan("$.amount", 100),
        jsonPathLessThan("$.retryCount", 5),
        
        // Assert that the request and response share the identical value for a specific field
        jsonFieldEquals("$.requestTraceId", "$.responseTraceId")
    )
```

### Regex Extraction
Ideal for unstructured Strings, logs, text-based legacy wire protocols, or when JSONPath is overkill. It uses standard Java Regular Expressions.

**Basic Pattern Matching:**
```java
kafka("Regex Extraction")
    // ...
    .check(
        // Extract capture group 1 and ensure it matches ACTIVE
        regex("status=([\\w]+)").find(1).is("ACTIVE"),
        
        // Ensure a specific pattern simply exists anywhere in the payload
        regex("CRITICAL_ERROR:\\s\\d+").exists()
    )
```

**Transformations:**
You can chain `.transform()` before the final validation to manipulate the extracted string using standard Java Lambdas.
```java
    .check(
        // Extract "balance=150.50", take group 1 ("150.50"), parse to Double, validate
        regex("balance=(\\d+\\.\\d+)")
            .find(1)
            .transform(Double::parseDouble)
            .gt(0.0),
            
        // Extract string, convert to uppercase, and validate
        regex("state=([a-z]+)")
            .find(1)
            .transform(String::toUpperCase)
            .is("PROCESSING")
    )
```

### XML / XPath
If your legacy systems deal with SOAP or raw XML over Kafka, the `xpath()` check provides full DOM parsing.

**Node and Attribute Extraction:**
```java
kafka("XML Pipeline")
    // ...
    .check(
        // Match exact text content of a node
        xpath("/response/status").find().is("SUCCESS"),
        
        // Ensure a node is not empty/zero
        xpath("/response/data/userId").find().not("0"),
        
        // Extract an XML attribute using the standard @ syntax
        xpath("/response/order/@id").find().saveAs("orderId")
    )
```

### Raw Substrings & Body
For basic and highly performant sanity checks when you don't need to parse the structure. These are significantly faster than JSONPath/XPath.

```java
kafka("Raw Body")
    .check(
        // Fast plain-text search anywhere in the payload
        substring("SYSTEM_OK").find().exists(),
        
        // Extract the entire payload as a raw String for custom lambda validation
        bodyString().transform(String::length).gt(10)
    )
```

---

## 2. Advanced: Request vs Response Comparison

Often, you must guarantee that the data coming back *exactly matches* mutations to the data you sent. Unlike simple Web Clients, this extension keeps the original Request in memory (via the RequestStore) when executing the checks on the Response.

You can wire custom Lambdas to evaluate them head-to-head.

```java
import pl.perfluencer.kafka.MessageCheck;

// ...
.check(
    new MessageCheck<>(
        "Data Integrity Compare",
        String.class, SerializationType.STRING,  // Request type
        String.class, SerializationType.STRING,  // Response type
        (reqBody, resBody) -> {
            boolean isMutated = parseStatus(resBody).equals("COMPLETED"));
            boolean includesSourceFlag = resBody.contains(extractIdentifier(reqBody));
            
            if (isMutated && includesSourceFlag) {
                return Optional.empty(); // Success requires empty Optionals!
            } else {
                return Optional.of("Data corruption detected on round trip!");
            }
        }
    )
)
```

### High-Speed String Checking Shortcuts
There are a handful of high-speed legacy shortcuts supported for raw payload checking.

```java
import static pl.perfluencer.common.checks.Checks.*;

.check(
    echoCheck(), // Verify response string equals request string exactly
    responseContainsRequest(), // Verify response contains the entire request string
    
    responseContains("SUCCESS"),
    responseMatches(".*CRITICAL.*"), // Regex matcher on the raw response string
    
    responseEquals("EXACT_MATCH"),
    responseNotEmpty()
)
```
