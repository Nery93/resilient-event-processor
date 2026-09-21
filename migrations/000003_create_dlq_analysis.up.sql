-- Create table for storing dead-letter queue (DLQ) analysis results

CREATE TABLE dlq_analysis(
    id SERIAL PRIMARY KEY,
    dlq_event_id INT NOT NULL REFERENCES dlq_events(id),
    analysis_result JSONB NOT NULL,
    analyzed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);