-- Create table for storing dead-letter queue (DLQ) events
CREATE TABLE dlq_events (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    payload JSONB NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    reason TEXT NOT NULL,
    failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);