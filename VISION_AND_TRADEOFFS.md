# Vision & Architectural Trade-offs: The "Resilience First" Approach

## Executive Summary

This framework is **not** a generic load generator. It is a **Resilience Measurement Instrument**.

Traditional performance tools (JMeter, K6, standard Gatling plugins) prioritize **Throughput** (Requests Per Second) at the expense of **Measurement Fidelity**. They are designed to "flood" a system.

This framework is designed to **audit** a system. It prioritizes **Data Integrity**, **Correctness**, and **State Consistency** above raw speed. It is built for scenarios where "losing a message" is an unacceptable failure mode (e.g., Payments, Banking, Critical Infrastructure).

---

## The Core Philosophy: "Trust, Then Verify"

In high-value transactional systems, the cost of a "False Positive" (reporting success when data was lost) or a "False Negative" (reporting failure when the test tool dropped the ball) is astronomical.

This framework adopts a **"Reference Monitor"** architecture. It acts as an independent, durable ledger that tracks the lifecycle of every single transaction with ACID-like properties.

### The "Test Invalidation" Principle

**Crucial Concept:** The **Request Store** (Postgres/Redis) is the "Black Box Flight Recorder" of the test.

*   **If the System Under Test (SUT) crashes:** The Request Store survives, allowing the test to verify if pending transactions were eventually processed or lost.
*   **If the Request Store crashes:** The test is **INVALID**.
    *   We do not attempt to "fail open" or "guess".
    *   If the ledger is compromised, the measurement is compromised. The test must be halted and repeated.
    *   *Justification:* You cannot audit a bank if the auditor's notebook is on fire.
    *   *Justification:* You cannot audit a bank if the auditor's notebook is on fire.

### Contextual Verification: The "Wrong Customer Data" Zero-Tolerance Policy

We enforce strict checks not just on the *presence* of a response, but on its *content* relative to the request.

*   **The Problem:** High-concurrency systems often suffer from race conditions or cache collisions where User A sees User B's data. A simple "200 OK" check misses this catastrophe.
*   **The Solution:** The framework allows (and encourages) deep inspection of the response payload against the *original request state* stored in the Request Store. We verify that the response isn't just "valid JSON", but that it is the *exact* answer to the specific question asked by this specific Virtual User.

---

## Architectural Trade-offs & Risk Analysis

We have made deliberate, controversial design choices to support this vision. Below is the risk analysis for each.

### 1. Blocking "At-Most-Once" Consumer Commit
**The Design:** The consumer commits the Kafka offset *synchronously* before the message is fully processed and asserted.
*   **Traditional View (Risk):** "This is slow (blocking network call) and risky (if the internal actor crashes immediately after commit, the message is lost/unasserted)."
*   **Resilience View (Benefit):**
    *   **Measurement Precision:** By blocking, we ensure that the "Time to Consume" metric reflects the *actual* availability of the consumer to take work, unmasked by internal buffering.
    *   **Chaos Visualization:** In chaos scenarios (e.g., broker failure), this behavior allows us to distinctively measure "Duplicate Delivery" vs. "Lost Message". If we used "At-Least-Once" (process then commit), we would generate our own duplicates during a crash, polluting the test data. We choose to let the **SUT** be the only source of duplicates.

### 2. Direct JDBC Inserts (No Batching)
**The Design:** Every request sent by a Virtual User triggers a direct, synchronous `INSERT` into PostgreSQL.
*   **Traditional View (Risk):** "This kills throughput. Database round-trips are expensive (1-5ms). You should batch inserts (e.g., every 100ms)."
*   **Resilience View (Benefit):**
    *   **Latency Fidelity:** Batching introduces "Jitter". A request waiting 90ms in a buffer has a fake 90ms added to its lifecycle. Direct inserts ensure **Zero Jitter**. The overhead is constant and predictable.
    *   **Race Condition Elimination:** In a Request-Reply flow, the response might arrive *before* the batch flushes. Direct inserts guarantee the "Request State" exists before the "Response" can possibly arrive.

### 3. Atomic State Management (Postgres CTEs)
**The Design:** Using `DELETE ... RETURNING` Common Table Expressions to match requests.
*   **Traditional View (Risk):** "Complex SQL, higher CPU load on the DB."
*   **Resilience View (Benefit):**
    *   **Distributed Correctness:** This solves the **"Split Brain"** problem. Multiple Gatling nodes can race to match responses, but the database guarantees **Exactly-Once Matching**.
    *   **Restartability:** If a Gatling node dies, another can pick up the work immediately without "double counting" transactions.

---

## Conclusion

This framework is designed for **High-Value Verification**. It is the tool you use when you need to prove to a regulator or a CTO that the system handles money correctly under failure.

However, it is **flexible**. While its default configuration prioritizes safety, it fully supports **Fire-and-Forget** modes where `acks=0` and `waitForAck=false`. In this mode, it acts as a highly efficient, actor-based load generator capable of saturating network bandwidth, similar to other tools. The power lies in the **choice**: you can have "Bank-Grade Auditing" for your payments flow and "Raw Throughput" for your logging flow, all within the same simulation.
