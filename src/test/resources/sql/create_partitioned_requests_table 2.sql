CREATE TABLE requests (
    correlation_id UUID NOT NULL,
    request_key VARCHAR(255),
    request_value_bytes BYTEA,
    serialization_type VARCHAR(50),
    transaction_name VARCHAR(255),
    scenario_name VARCHAR(255),
    start_time TIMESTAMP,
    timeout_time TIMESTAMP,
    expired BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (correlation_id)
) PARTITION BY HASH (correlation_id);

-- Create 48 partitions
DO $$
BEGIN
    FOR i IN 0..47 LOOP
        EXECUTE format('CREATE TABLE requests_p%s PARTITION OF requests FOR VALUES WITH (MODULUS 48, REMAINDER %s)', i, i);
    END LOOP;
END $$;
